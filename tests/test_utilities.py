import logging
import uuid
from typing import Type
from unittest import mock

import kubernetes
import pytest
import urllib3
from kubernetes.client import models as k8s_models
from prefect.infrastructure.kubernetes import KubernetesJob

from prefect_kubernetes.utilities import (
    ResilientStreamWatcher,
    convert_manifest_to_model,
)

base_path = "tests/sample_k8s_resources"

sample_deployment_manifest = KubernetesJob.job_from_file(
    f"{base_path}/sample_deployment.yaml"
)
sample_job_manifest = KubernetesJob.job_from_file(f"{base_path}/sample_job.yaml")
sample_pod_manifest = KubernetesJob.job_from_file(f"{base_path}/sample_pod.yaml")
sample_service_manifest = KubernetesJob.job_from_file(
    f"{base_path}/sample_service.yaml"
)

expected_deployment_model = k8s_models.V1Deployment(
    **dict(
        api_version="apps/v1",
        kind="Deployment",
        metadata=k8s_models.V1ObjectMeta(
            **dict(
                name="nginx-deployment",
                labels={"app": "nginx"},
            )
        ),
        spec=k8s_models.V1DeploymentSpec(
            **dict(
                replicas=3,
                selector=k8s_models.V1LabelSelector(
                    **dict(
                        match_labels={"app": "nginx"},
                    )
                ),
                template=k8s_models.V1PodTemplateSpec(
                    **dict(
                        metadata=k8s_models.V1ObjectMeta(
                            **dict(
                                labels={"app": "nginx"},
                            )
                        ),
                        spec=k8s_models.V1PodSpec(
                            **dict(
                                containers=[
                                    k8s_models.V1Container(
                                        **dict(
                                            name="nginx",
                                            image="nginx:1.14.2",
                                            ports=[
                                                k8s_models.V1ContainerPort(
                                                    **dict(container_port=80)
                                                )
                                            ],
                                        )
                                    )
                                ]
                            )
                        ),
                    )
                ),
            )
        ),
    )
)

expected_pod_model = k8s_models.V1Pod(
    **dict(
        api_version="v1",
        kind="Pod",
        metadata=k8s_models.V1ObjectMeta(**dict(name="nginx")),
        spec=k8s_models.V1PodSpec(
            **dict(
                containers=[
                    k8s_models.V1Container(
                        **dict(
                            name="nginx",
                            image="nginx:1.14.2",
                            ports=[
                                k8s_models.V1ContainerPort(**dict(container_port=80))
                            ],
                        )
                    )
                ]
            )
        ),
    )
)

expected_job_model = k8s_models.V1Job(
    **dict(
        api_version="batch/v1",
        kind="Job",
        metadata=k8s_models.V1ObjectMeta(
            **dict(
                name="pi",
            )
        ),
        spec=k8s_models.V1JobSpec(
            **dict(
                template=k8s_models.V1PodTemplateSpec(
                    **dict(
                        spec=k8s_models.V1PodSpec(
                            **dict(
                                containers=[
                                    k8s_models.V1Container(
                                        **dict(
                                            name="pi",
                                            image="perl:5.34.0",
                                            command=[
                                                "perl",
                                                "-Mbignum=bpi",
                                                "-wle",
                                                "print bpi(2000)",
                                            ],
                                        )
                                    )
                                ],
                                restart_policy="Never",
                            )
                        ),
                    )
                ),
                backoff_limit=4,
            )
        ),
    )
)

expected_service_model = k8s_models.V1Service(
    **dict(
        api_version="v1",
        kind="Service",
        metadata=k8s_models.V1ObjectMeta(
            **dict(
                name="nginx-service",
            )
        ),
        spec=k8s_models.V1ServiceSpec(
            **dict(
                selector={"app.kubernetes.io/name": "proxy"},
                ports=[
                    k8s_models.V1ServicePort(
                        **dict(
                            name="name-of-service-port",
                            protocol="TCP",
                            port=80,
                            target_port="http-web-svc",
                        )
                    )
                ],
            )
        ),
    )
)


@pytest.mark.parametrize(
    "manifest,model_name,expected_model",
    [
        (
            f"{base_path}/sample_deployment.yaml",
            "V1Deployment",
            expected_deployment_model,
        ),
        (sample_deployment_manifest, "V1Deployment", expected_deployment_model),
        (f"{base_path}/sample_pod.yaml", "V1Pod", expected_pod_model),
        (sample_pod_manifest, "V1Pod", expected_pod_model),
        (f"{base_path}/sample_job.yaml", "V1Job", expected_job_model),
        (sample_job_manifest, "V1Job", expected_job_model),
        (f"{base_path}/sample_service.yaml", "V1Service", expected_service_model),
        (sample_service_manifest, "V1Service", expected_service_model),
    ],
)
def test_convert_manifest_to_model(manifest, model_name, expected_model):
    v1_model = convert_manifest_to_model(manifest, model_name)

    assert isinstance(v1_model, getattr(k8s_models, model_name))

    assert v1_model == expected_model


def test_bad_manifest_filename_raises():
    with pytest.raises(
        ValueError, match="Manifest must be a valid dict or path to a .yaml file."
    ):
        convert_manifest_to_model("isaid86753OHnahhhEEEIIIEEEYEN", "V1Deployment")


@pytest.mark.parametrize(
    "v1_model_name",
    [
        "V1Schledloyment",
        ["V1Deployment"],
    ],
)
def test_bad_model_type_raises(v1_model_name):
    with pytest.raises(
        ValueError,
        match="`v1_model` must be the name of a valid Kubernetes client model.",
    ):
        convert_manifest_to_model(sample_deployment_manifest, v1_model_name)


def test_resilient_streaming_retries_on_configured_errors(caplog):
    watcher = ResilientStreamWatcher(logger=logging.getLogger("test"))

    with mock.patch.object(
        watcher.watch,
        "stream",
        side_effect=[
            watcher.reconnect_exceptions[0],
            watcher.reconnect_exceptions[0],
            ["random_success"],
        ],
    ) as mocked_stream:
        for log in watcher.api_object_stream(str):
            assert log == "random_success"

    assert mocked_stream.call_count == 3
    assert "Unable to connect, retrying..." in caplog.text


@pytest.mark.parametrize(
    "exc", [Exception, TypeError, ValueError, urllib3.exceptions.ProtocolError]
)
def test_resilient_streaming_raises_on_unconfigured_errors(
    exc: Type[Exception], caplog
):
    watcher = ResilientStreamWatcher(
        logger=logging.getLogger("test"), reconnect_exceptions=[]
    )

    with mock.patch.object(watcher.watch, "stream", side_effect=[exc]) as mocked_stream:
        with pytest.raises(exc):
            for _ in watcher.api_object_stream(str):
                pass

    assert mocked_stream.call_count == 1
    assert "Unexpected error" in caplog.text
    assert exc.__name__ in caplog.text


def _create_api_objects_mocks(n: int = 3):
    objects = []
    for _ in range(n):
        o = mock.MagicMock(spec=kubernetes.client.V1Pod)
        o.metadata = mock.PropertyMock()
        o.metadata.uid = uuid.uuid4()
        objects.append(o)
    return objects


def test_resilient_streaming_deduplicates_api_objects_on_reconnects():
    watcher = ResilientStreamWatcher(logger=logging.getLogger("test"))

    object_pool = _create_api_objects_mocks()
    thrown_exceptions = 0

    def my_stream(*args, **kwargs):
        """
        Simulate a stream that throws exceptions after yielding the first
        object before yielding the rest of the objects.
        """
        for o in object_pool:
            yield {"object": o}

            nonlocal thrown_exceptions
            if thrown_exceptions < 3:
                thrown_exceptions += 1
                raise watcher.reconnect_exceptions[0]

    watcher.watch.stream = my_stream
    results = [obj for obj in watcher.api_object_stream(str)]

    assert len(object_pool) == len(results)


def test_resilient_streaming_pulls_all_logs_on_reconnects():
    watcher = ResilientStreamWatcher(logger=logging.getLogger("test"))

    logs = ["log1", "log2", "log3", "log4"]
    thrown_exceptions = 0

    def my_stream(*args, **kwargs):
        """
        Simulate a stream that throws exceptions after yielding the first
        object before yielding the rest of the objects.
        """
        for log in logs:
            yield log

            nonlocal thrown_exceptions
            if thrown_exceptions < 3:
                thrown_exceptions += 1
                raise watcher.reconnect_exceptions[0]

    watcher.watch.stream = my_stream
    results = [obj for obj in watcher.stream(str)]

    assert results == [
        "log1",  # Before first exception
        "log1",  # Before second exception
        "log1",  # Before third exception
        "log1",  # No more exceptions from here onward
        "log2",
        "log3",
        "log4",
    ]
