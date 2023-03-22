from contextlib import contextmanager
from time import monotonic
from unittest import mock
from unittest.mock import MagicMock, Mock

import anyio
import anyio.abc
import kubernetes
import pendulum
import prefect
import pytest
from kubernetes.client.exceptions import ApiException
from kubernetes.config import ConfigException
from prefect.client.schemas import FlowRun
from prefect.docker import get_prefect_image_name
from prefect.server.schemas.core import Flow
from prefect.server.schemas.responses import DeploymentResponse
from prefect.settings import (
    PREFECT_EXPERIMENTAL_ENABLE_WORKERS,
    PREFECT_EXPERIMENTAL_WARN_WORKERS,
    get_current_settings,
    temporary_settings,
)

from prefect_kubernetes import KubernetesWorker
from prefect_kubernetes.utilities import _slugify_label_value
from prefect_kubernetes.worker import KubernetesWorkerJobConfiguration

FAKE_CLUSTER = "fake-cluster"
MOCK_CLUSTER_UID = "1234"


@pytest.fixture(autouse=True)
def enable_workers():
    with temporary_settings(
        {PREFECT_EXPERIMENTAL_ENABLE_WORKERS: 1, PREFECT_EXPERIMENTAL_WARN_WORKERS: 0}
    ):
        yield


@pytest.fixture
def mock_watch(monkeypatch):
    pytest.importorskip("kubernetes")

    mock = MagicMock()

    monkeypatch.setattr("kubernetes.watch.Watch", MagicMock(return_value=mock))
    return mock


@pytest.fixture
def mock_cluster_config(monkeypatch):
    mock = MagicMock()
    # We cannot mock this or the `except` clause will complain
    mock.config.ConfigException = ConfigException
    mock.list_kube_config_contexts.return_value = (
        [],
        {"context": {"cluster": FAKE_CLUSTER}},
    )
    monkeypatch.setattr("kubernetes.config", mock)
    monkeypatch.setattr("kubernetes.config.ConfigException", ConfigException)
    return mock


@pytest.fixture
def mock_anyio_sleep_monotonic(monkeypatch):
    def mock_monotonic():
        return mock_sleep.current_time

    def mock_sleep(duration):
        mock_sleep.current_time += duration

    mock_sleep.current_time = monotonic()
    monkeypatch.setattr("time.monotonic", mock_monotonic)
    monkeypatch.setattr("anyio.sleep", mock_sleep)


@pytest.fixture
def mock_job():
    mock = MagicMock(spec=kubernetes.client.V1Job)
    mock.metadata.name = "mock-job"
    mock.metadata.namespace = "mock-namespace"
    return mock


@pytest.fixture
def mock_core_client(monkeypatch, mock_cluster_config):
    mock = MagicMock(spec=kubernetes.client.CoreV1Api)
    mock.read_namespace.return_value.metadata.uid = MOCK_CLUSTER_UID

    @contextmanager
    def get_core_client(*args, **kwargs):
        yield mock

    monkeypatch.setattr(
        "prefect_kubernetes.worker.KubernetesWorker._get_core_client",
        get_core_client,
    )
    return mock


@pytest.fixture
def mock_batch_client(monkeypatch, mock_cluster_config, mock_job):
    pytest.importorskip("kubernetes")

    mock = MagicMock(spec=kubernetes.client.BatchV1Api)
    mock.read_namespaced_job.return_value = mock_job
    mock.create_namespaced_job.return_value = mock_job

    @contextmanager
    def get_batch_client(*args, **kwargs):
        yield mock

    monkeypatch.setattr(
        "prefect_kubernetes.worker.KubernetesWorker._get_batch_client",
        get_batch_client,
    )
    return mock


def _mock_pods_stream_that_returns_running_pod(*args, **kwargs):
    job_pod = MagicMock(spec=kubernetes.client.V1Pod)
    job_pod.status.phase = "Running"

    job = MagicMock(spec=kubernetes.client.V1Job)
    job.status.completion_time = pendulum.now("utc").timestamp()

    return [{"object": job_pod}, {"object": job}]


from_template_and_values_cases = [
    (
        # default base template with no values
        KubernetesWorker.get_default_base_job_template(),
        {},
        KubernetesWorkerJobConfiguration(
            command=None,
            env={},
            labels={},
            name=None,
            namespace="default",
            job_manifest={
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"namespace": "default", "generateName": "{{ name }}-"},
                "spec": {
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "name": "prefect-job",
                                    "imagePullPolicy": "IfNotPresent",
                                }
                            ],
                        }
                    }
                },
            },
            cluster_config=None,
            job_watch_timeout_seconds=None,
            pod_watch_timeout_seconds=60,
            stream_output=True,
        ),
        lambda flow_run, deployment, flow: KubernetesWorkerJobConfiguration(
            command="python -m prefect.engine",
            env={
                **get_current_settings().to_environment_variables(exclude_unset=True),
                "PREFECT__FLOW_RUN_ID": flow_run.id.hex,
            },
            labels={
                "prefect.io/flow-run-id": str(flow_run.id),
                "prefect.io/flow-run-name": flow_run.name,
                "prefect.io/version": prefect.__version__,
                "prefect.io/deployment-name": deployment.name,
                "prefect.io/flow-name": flow.name,
            },
            name=flow_run.name,
            namespace="default",
            job_manifest={
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "namespace": "default",
                    "generateName": f"{flow_run.name}-",
                    "labels": {
                        "prefect.io/flow-run-id": str(flow_run.id),
                        "prefect.io/flow-run-name": flow_run.name,
                        "prefect.io/version": _slugify_label_value(prefect.__version__),
                        "prefect.io/deployment-name": deployment.name,
                        "prefect.io/flow-name": flow.name,
                    },
                },
                "spec": {
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "name": "prefect-job",
                                    "imagePullPolicy": "IfNotPresent",
                                    "env": [
                                        *[
                                            {"name": k, "value": v}
                                            for k, v in get_current_settings()
                                            .to_environment_variables(
                                                exclude_unset=True
                                            )
                                            .items()
                                        ],
                                        {
                                            "name": "PREFECT__FLOW_RUN_ID",
                                            "value": flow_run.id.hex,
                                        },
                                    ],
                                    "image": get_prefect_image_name(),
                                    "args": ["python", "-m", "prefect.engine"],
                                }
                            ],
                        }
                    }
                },
            },
            cluster_config=None,
            job_watch_timeout_seconds=None,
            pod_watch_timeout_seconds=60,
            stream_output=True,
        ),
    ),
    (
        # default base template with no values
        KubernetesWorker.get_default_base_job_template(),
        {
            "name": "test",
            "job_watch_timeout_seconds": 120,
            "pod_watch_timeout_seconds": 90,
            "stream_output": False,
            "env": {
                "TEST_ENV": "test",
            },
            "labels": {
                "TEST_LABEL": "test label",
            },
            "service_account_name": "test-service-account",
            "image_pull_policy": "Always",
            "command": "echo hello",
            "image": "test-image:latest",
            "finished_job_ttl": 60,
            "namespace": "test-namespace",
        },
        KubernetesWorkerJobConfiguration(
            command="echo hello",
            env={
                "TEST_ENV": "test",
            },
            labels={
                "TEST_LABEL": "test label",
            },
            name="test",
            namespace="test-namespace",
            job_manifest={
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "labels": {"TEST_LABEL": "test label"},
                    "namespace": "test-namespace",
                    "generateName": "test-",
                },
                "spec": {
                    "ttlSecondsAfterFinished": 60,
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "restartPolicy": "Never",
                            "serviceAccountName": "test-service-account",
                            "containers": [
                                {
                                    "name": "prefect-job",
                                    "env": {
                                        "TEST_ENV": "test",
                                    },
                                    "image": "test-image:latest",
                                    "imagePullPolicy": "Always",
                                    "args": "echo hello",
                                }
                            ],
                        }
                    },
                },
            },
            cluster_config=None,
            job_watch_timeout_seconds=120,
            pod_watch_timeout_seconds=90,
            stream_output=False,
        ),
        lambda flow_run, deployment, flow: KubernetesWorkerJobConfiguration(
            command="echo hello",
            env={
                **get_current_settings().to_environment_variables(exclude_unset=True),
                "PREFECT__FLOW_RUN_ID": flow_run.id.hex,
                "TEST_ENV": "test",
            },
            labels={
                "prefect.io/flow-run-id": str(flow_run.id),
                "prefect.io/flow-run-name": flow_run.name,
                "prefect.io/version": prefect.__version__,
                "prefect.io/deployment-name": deployment.name,
                "prefect.io/flow-name": flow.name,
                "TEST_LABEL": "test label",
            },
            name="test",
            namespace="test-namespace",
            job_manifest={
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "namespace": "test-namespace",
                    "generateName": "test-",
                    "labels": {
                        "prefect.io/flow-run-id": str(flow_run.id),
                        "prefect.io/flow-run-name": flow_run.name,
                        "prefect.io/version": _slugify_label_value(prefect.__version__),
                        "prefect.io/deployment-name": deployment.name,
                        "prefect.io/flow-name": flow.name,
                        "test_label": "test-label",
                    },
                },
                "spec": {
                    "ttlSecondsAfterFinished": 60,
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "restartPolicy": "Never",
                            "serviceAccountName": "test-service-account",
                            "containers": [
                                {
                                    "name": "prefect-job",
                                    "imagePullPolicy": "Always",
                                    "env": [
                                        *[
                                            {"name": k, "value": v}
                                            for k, v in get_current_settings()
                                            .to_environment_variables(
                                                exclude_unset=True
                                            )
                                            .items()
                                        ],
                                        {
                                            "name": "PREFECT__FLOW_RUN_ID",
                                            "value": flow_run.id.hex,
                                        },
                                        {
                                            "name": "TEST_ENV",
                                            "value": "test",
                                        },
                                    ],
                                    "image": "test-image:latest",
                                    "args": ["echo", "hello"],
                                }
                            ],
                        }
                    },
                },
            },
            cluster_config=None,
            job_watch_timeout_seconds=120,
            pod_watch_timeout_seconds=90,
            stream_output=False,
        ),
    ),
    # custom template with values
    (
        {
            "job_configuration": {
                "command": "{{ command }}",
                "env": "{{ env }}",
                "labels": "{{ labels }}",
                "name": "{{ name }}",
                "namespace": "{{ namespace }}",
                "job_manifest": {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "spec": {
                        "template": {
                            "spec": {
                                "parallelism": 1,
                                "completions": 1,
                                "restartPolicy": "Never",
                                "containers": [
                                    {
                                        "name": "prefect-job",
                                        "image": "{{ image }}",
                                        "imagePullPolicy": "{{ image_pull_policy }}",
                                        "args": "{{ command }}",
                                        "resources": {
                                            "requests": {"memory": "{{ memory }}Mi"},
                                            "limits": {"memory": "200Mi"},
                                        },
                                    }
                                ],
                            }
                        }
                    },
                },
                "cluster_config": "{{ cluster_config }}",
                "job_watch_timeout_seconds": "{{ job_watch_timeout_seconds }}",
                "pod_watch_timeout_seconds": "{{ pod_watch_timeout_seconds }}",
                "stream_output": "{{ stream_output }}",
            },
            "variables": {
                "type": "object",
                "properties": {
                    "name": {
                        "title": "Name",
                        "description": "Name given to infrastructure created by a worker.",
                        "type": "string",
                    },
                    "env": {
                        "title": "Environment Variables",
                        "description": "Environment variables to set when starting a flow run.",
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                    "labels": {
                        "title": "Labels",
                        "description": "Labels applied to infrastructure created by a worker.",
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                    "command": {
                        "title": "Command",
                        "description": "The command to use when starting a flow run. In most cases, this should be left blank and the command will be automatically generated by the worker.",
                        "type": "string",
                    },
                    "namespace": {
                        "title": "Namespace",
                        "description": "The Kubernetes namespace to create jobs within.",
                        "default": "default",
                        "type": "string",
                    },
                    "image": {
                        "title": "Image",
                        "description": "The image reference of a container image to use for created jobs. If not set, the latest Prefect image will be used.",
                        "example": "docker.io/prefecthq/prefect:2-latest",
                        "type": "string",
                    },
                    "image_pull_policy": {
                        "title": "Image Pull Policy",
                        "description": "The Kubernetes image pull policy to use for job containers.",
                        "default": "IfNotPresent",
                        "enum": ["IfNotPresent", "Always", "Never"],
                        "type": "string",
                    },
                    "job_watch_timeout_seconds": {
                        "title": "Job Watch Timeout Seconds",
                        "description": "Number of seconds to wait for each event emitted by a job before timing out. If not set, the worker will wait for each event indefinitely.",
                        "type": "integer",
                    },
                    "pod_watch_timeout_seconds": {
                        "title": "Pod Watch Timeout Seconds",
                        "description": "Number of seconds to watch for pod creation before timing out.",
                        "default": 60,
                        "type": "integer",
                    },
                    "stream_output": {
                        "title": "Stream Output",
                        "description": "If set, output will be streamed from the job to local standard output.",
                        "default": True,
                        "type": "boolean",
                    },
                    "cluster_config": {
                        "title": "Cluster Config",
                        "description": "The Kubernetes cluster config to use for job creation.",
                        "allOf": [{"$ref": "#/definitions/KubernetesClusterConfig"}],
                    },
                    "memory": {
                        "title": "Memory",
                        "description": "The amount of memory to use for each job in MiB",
                        "default": 100,
                        "type": "number",
                        "min": 0,
                        "max": 200,
                    },
                },
                "definitions": {
                    "KubernetesClusterConfig": {
                        "title": "KubernetesClusterConfig",
                        "description": "Stores configuration for interaction with Kubernetes clusters.\n\nSee `from_file` for creation.",
                        "type": "object",
                        "properties": {
                            "config": {
                                "title": "Config",
                                "description": "The entire contents of a kubectl config file.",
                                "type": "object",
                            },
                            "context_name": {
                                "title": "Context Name",
                                "description": "The name of the kubectl context to use.",
                                "type": "string",
                            },
                        },
                        "required": ["config", "context_name"],
                        "block_type_slug": "kubernetes-cluster-config",
                        "secret_fields": [],
                        "block_schema_references": {},
                    }
                },
            },
        },
        {
            "name": "test",
            "job_watch_timeout_seconds": 120,
            "pod_watch_timeout_seconds": 90,
            "env": {
                "TEST_ENV": "test",
            },
            "labels": {
                "TEST_LABEL": "test label",
            },
            "image_pull_policy": "Always",
            "command": "echo hello",
            "image": "test-image:latest",
        },
        KubernetesWorkerJobConfiguration(
            command="echo hello",
            env={
                "TEST_ENV": "test",
            },
            labels={
                "TEST_LABEL": "test label",
            },
            name="test",
            namespace="default",
            job_manifest={
                "apiVersion": "batch/v1",
                "kind": "Job",
                "spec": {
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "name": "prefect-job",
                                    "image": "test-image:latest",
                                    "imagePullPolicy": "Always",
                                    "args": "echo hello",
                                    "resources": {
                                        "limits": {
                                            "memory": "200Mi",
                                        },
                                        "requests": {
                                            "memory": "100Mi",
                                        },
                                    },
                                },
                            ],
                        }
                    }
                },
            },
            cluster_config=None,
            job_watch_timeout_seconds=120,
            pod_watch_timeout_seconds=90,
            stream_output=True,
        ),
        lambda flow_run, deployment, flow: KubernetesWorkerJobConfiguration(
            command="echo hello",
            env={
                **get_current_settings().to_environment_variables(exclude_unset=True),
                "PREFECT__FLOW_RUN_ID": flow_run.id.hex,
                "TEST_ENV": "test",
            },
            labels={
                "prefect.io/flow-run-id": str(flow_run.id),
                "prefect.io/flow-run-name": flow_run.name,
                "prefect.io/version": prefect.__version__,
                "prefect.io/deployment-name": deployment.name,
                "prefect.io/flow-name": flow.name,
                "TEST_LABEL": "test label",
            },
            name="test",
            namespace="default",
            job_manifest={
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "namespace": "default",
                    "generateName": "test-",
                    "labels": {
                        "prefect.io/flow-run-id": str(flow_run.id),
                        "prefect.io/flow-run-name": flow_run.name,
                        "prefect.io/version": _slugify_label_value(prefect.__version__),
                        "prefect.io/deployment-name": deployment.name,
                        "prefect.io/flow-name": flow.name,
                        "test_label": "test-label",
                    },
                },
                "spec": {
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "name": "prefect-job",
                                    "imagePullPolicy": "Always",
                                    "env": [
                                        *[
                                            {"name": k, "value": v}
                                            for k, v in get_current_settings()
                                            .to_environment_variables(
                                                exclude_unset=True
                                            )
                                            .items()
                                        ],
                                        {
                                            "name": "PREFECT__FLOW_RUN_ID",
                                            "value": flow_run.id.hex,
                                        },
                                        {
                                            "name": "TEST_ENV",
                                            "value": "test",
                                        },
                                    ],
                                    "image": "test-image:latest",
                                    "args": ["echo", "hello"],
                                    "resources": {
                                        "limits": {
                                            "memory": "200Mi",
                                        },
                                        "requests": {
                                            "memory": "100Mi",
                                        },
                                    },
                                }
                            ],
                        }
                    }
                },
            },
            cluster_config=None,
            job_watch_timeout_seconds=120,
            pod_watch_timeout_seconds=90,
            stream_output=True,
        ),
    ),
]


class TestKubernetesWorkerJobConfiguration:
    @pytest.fixture
    def flow_run(self):
        return FlowRun(name="my-flow-run-name")

    @pytest.fixture
    def deployment(self):
        return DeploymentResponse(name="my-deployment-name")

    @pytest.fixture
    def flow(self):
        return Flow(name="my-flow-name")

    @pytest.mark.parametrize(
        "template,values,expected_after_template,expected_after_preparation",
        from_template_and_values_cases,
    )
    async def test_job_configuration_preparation(
        self,
        template,
        values,
        expected_after_template,
        expected_after_preparation,
        flow_run,
        deployment,
        flow,
    ):
        """Tests that the job configuration is correctly templated and prepared."""
        result = await KubernetesWorkerJobConfiguration.from_template_and_values(
            base_job_template=template,
            values=values,
        )
        # comparing dictionaries produces cleaner diffs
        assert result.dict() == expected_after_template.dict()

        result.prepare_for_flow_run(flow_run=flow_run, deployment=deployment, flow=flow)

        assert (
            result.dict()
            == expected_after_preparation(flow_run, deployment, flow).dict()
        )


class TestKubernetesWorker:
    @pytest.fixture
    async def default_configuration(self):
        return await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(), {}
        )

    @pytest.fixture
    def flow_run(self):
        return FlowRun(name="my-flow-run-name")

    async def test_creates_job_by_building_a_manifest(
        self,
        default_configuration: KubernetesWorkerJobConfiguration,
        flow_run,
        mock_batch_client,
        mock_core_client,
        mock_watch,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod
        default_configuration.prepare_for_flow_run(flow_run)
        expected_manifest = default_configuration.job_manifest

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run=flow_run, configuration=default_configuration)
            mock_core_client.read_namespaced_pod_status.assert_called_once()

            mock_batch_client.create_namespaced_job.assert_called_with(
                "default",
                expected_manifest,
            )

    async def test_task_status_receives_job_pid(
        self,
        default_configuration: KubernetesWorkerJobConfiguration,
        flow_run,
        mock_batch_client,
        mock_core_client,
        mock_watch,
        monkeypatch,
    ):
        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            fake_status = MagicMock(spec=anyio.abc.TaskStatus)
            await k8s_worker.run(
                flow_run=flow_run,
                configuration=default_configuration,
                task_status=fake_status,
            )
            expected_value = f"{MOCK_CLUSTER_UID}:mock-namespace:mock-job"
            fake_status.started.assert_called_once_with(expected_value)

    async def test_cluster_uid_uses_env_var_if_set(
        self,
        default_configuration: KubernetesWorkerJobConfiguration,
        flow_run,
        mock_batch_client,
        mock_core_client,
        mock_watch,
        monkeypatch,
    ):
        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            monkeypatch.setenv("PREFECT_KUBERNETES_CLUSTER_UID", "test-uid")
            fake_status = MagicMock(spec=anyio.abc.TaskStatus)
            result = await k8s_worker.run(
                flow_run=flow_run,
                configuration=default_configuration,
                task_status=fake_status,
            )

            mock_core_client.read_namespace.assert_not_called()
            expected_value = "test-uid:mock-namespace:mock-job"
            assert result.identifier == expected_value
            fake_status.started.assert_called_once_with(expected_value)

    async def test_task_group_start_returns_job_pid(
        self,
        flow_run,
        default_configuration: KubernetesWorkerJobConfiguration,
        mock_batch_client,
        mock_core_client,
        mock_watch,
        monkeypatch,
    ):
        expected_value = f"{MOCK_CLUSTER_UID}:mock-namespace:mock-job"
        async with anyio.create_task_group() as tg:
            async with KubernetesWorker(work_pool_name="test") as k8s_worker:
                result = await tg.start(k8s_worker.run, flow_run, default_configuration)
                assert result == expected_value

    async def test_missing_job_returns_bad_status_code(
        self,
        flow_run,
        default_configuration: KubernetesWorkerJobConfiguration,
        mock_batch_client,
        mock_core_client,
        mock_watch,
        caplog,
    ):
        mock_batch_client.read_namespaced_job.side_effect = ApiException(
            status=404, reason="Job not found"
        )

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            result = await k8s_worker.run(
                flow_run=flow_run, configuration=default_configuration
            )

            _, _, job_name = k8s_worker._parse_infrastructure_pid(result.identifier)

            assert result.status_code == -1
            assert f"Job {job_name!r} was removed" in caplog.text

    @pytest.mark.parametrize(
        "job_name,clean_name",
        [
            ("infra-run", "infra-run-"),
            ("infra-run-", "infra-run-"),
            ("_infra_run", "infra-run-"),
            ("...infra_run", "infra-run-"),
            ("._-infra_run", "infra-run-"),
            ("9infra-run", "9infra-run-"),
            ("-infra.run", "infra-run-"),
            ("infra*run", "infra-run-"),
            ("infra9.-foo_bar^x", "infra9-foo-bar-x-"),
        ],
    )
    async def test_job_name_creates_valid_name(
        self,
        default_configuration: KubernetesWorkerJobConfiguration,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
        job_name,
        clean_name,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod
        default_configuration.name = job_name
        default_configuration.prepare_for_flow_run(flow_run)
        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run=flow_run, configuration=default_configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            call_name = mock_batch_client.create_namespaced_job.call_args[0][1][
                "metadata"
            ]["generateName"]
            assert call_name == clean_name

    async def test_uses_image_variable(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod

        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(), {"image": "foo"}
        )
        async with KubernetesWorker(work_pool_name="test") as k8s_worker:

            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            image = mock_batch_client.create_namespaced_job.call_args[0][1]["spec"][
                "template"
            ]["spec"]["containers"][0]["image"]
            assert image == "foo"

    async def test_allows_image_setting_from_manifest(
        self,
        default_configuration: KubernetesWorkerJobConfiguration,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod

        default_configuration.job_manifest["spec"]["template"]["spec"]["containers"][0][
            "image"
        ] = "test"
        default_configuration.prepare_for_flow_run(flow_run)

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, default_configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            image = mock_batch_client.create_namespaced_job.call_args[0][1]["spec"][
                "template"
            ]["spec"]["containers"][0]["image"]
            assert image == "test"

    async def test_uses_labels_setting(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod

        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(),
            {"labels": {"foo": "foo", "bar": "bar"}},
        )

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            labels = mock_batch_client.create_namespaced_job.call_args[0][1][
                "metadata"
            ]["labels"]
            assert labels["foo"] == "foo"
            assert labels["bar"] == "bar"

    async def test_sets_environment_variables(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(),
            {"env": {"foo": "FOO", "bar": "BAR"}},
        )
        configuration.prepare_for_flow_run(flow_run)

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()

            manifest = mock_batch_client.create_namespaced_job.call_args[0][1]
            pod = manifest["spec"]["template"]["spec"]
            env = pod["containers"][0]["env"]
            assert env == [
                {"name": key, "value": value}
                for key, value in {
                    **configuration._base_environment(),
                    **configuration._base_flow_run_environment(flow_run),
                    "foo": "FOO",
                    "bar": "BAR",
                }.items()
            ]

    async def test_allows_unsetting_environment_variables(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(),
            {"env": {"PREFECT_TEST_MODE": None}},
        )
        configuration.prepare_for_flow_run(flow_run)
        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()

            manifest = mock_batch_client.create_namespaced_job.call_args[0][1]
            pod = manifest["spec"]["template"]["spec"]
            env = pod["containers"][0]["env"]
            env_names = {variable["name"] for variable in env}
            assert "PREFECT_TEST_MODE" not in env_names

    @pytest.mark.parametrize(
        "given,expected",
        [
            ("a-valid-dns-subdomain1/and-a-name", "a-valid-dns-subdomain1/and-a-name"),
            (
                "a-prefix-with-invalid$@*^$@-characters/and-a-name",
                "a-prefix-with-invalid-characters/and-a-name",
            ),
            (
                "a-name-with-invalid$@*^$@-characters",
                "a-name-with-invalid-characters",
            ),
            ("/a-name-that-starts-with-slash", "a-name-that-starts-with-slash"),
            ("a-prefix/and-a-name/-with-a-slash", "a-prefix/and-a-name-with-a-slash"),
            (
                "_a-name-that-starts-with-underscore",
                "a-name-that-starts-with-underscore",
            ),
            ("-a-name-that-starts-with-dash", "a-name-that-starts-with-dash"),
            (".a-name-that-starts-with-period", "a-name-that-starts-with-period"),
            ("a-name-that-ends-with-underscore_", "a-name-that-ends-with-underscore"),
            ("a-name-that-ends-with-dash-", "a-name-that-ends-with-dash"),
            ("a-name-that-ends-with-period.", "a-name-that-ends-with-period"),
            (
                "._.-a-name-with-trailing-leading-chars-__-.",
                "a-name-with-trailing-leading-chars",
            ),
            ("a-prefix/and-a-name/-with-a-slash", "a-prefix/and-a-name-with-a-slash"),
            # Truncation of the prefix
            ("a" * 300 + "/and-a-name", "a" * 253 + "/and-a-name"),
            # Truncation of the name
            ("a" * 300, "a" * 63),
            # Truncation of the prefix and name together
            ("a" * 300 + "/" + "b" * 100, "a" * 253 + "/" + "b" * 63),
            # All invalid passes through
            ("$@*^$@", "$@*^$@"),
            # All invalid passes through for prefix
            ("$@*^$@/name", "$@*^$@/name"),
        ],
    )
    async def test_sanitizes_user_label_keys(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
        given,
        expected,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod
        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(),
            {"labels": {given: "foo"}},
        )
        configuration.prepare_for_flow_run(flow_run)

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            labels = mock_batch_client.create_namespaced_job.call_args[0][1][
                "metadata"
            ]["labels"]
            assert labels[expected] == "foo"

    @pytest.mark.parametrize(
        "given,expected",
        [
            ("valid-label-text", "valid-label-text"),
            (
                "text-with-invalid$@*^$@-characters",
                "text-with-invalid-characters",
            ),
            ("_value-that-starts-with-underscore", "value-that-starts-with-underscore"),
            ("-value-that-starts-with-dash", "value-that-starts-with-dash"),
            (".value-that-starts-with-period", "value-that-starts-with-period"),
            ("value-that-ends-with-underscore_", "value-that-ends-with-underscore"),
            ("value-that-ends-with-dash-", "value-that-ends-with-dash"),
            ("value-that-ends-with-period.", "value-that-ends-with-period"),
            (
                "._.-value-with-trailing-leading-chars-__-.",
                "value-with-trailing-leading-chars",
            ),
            # Truncation
            ("a" * 100, "a" * 63),
            # All invalid passes through
            ("$@*^$@", "$@*^$@"),
        ],
    )
    async def test_sanitizes_user_label_values(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
        given,
        expected,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod

        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(),
            {"labels": {"foo": given}},
        )
        configuration.prepare_for_flow_run(flow_run)

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            labels = mock_batch_client.create_namespaced_job.call_args[0][1][
                "metadata"
            ]["labels"]
            assert labels["foo"] == expected

    async def test_uses_namespace_setting(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod
        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(),
            {"namespace": "foo"},
        )

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            namespace = mock_batch_client.create_namespaced_job.call_args[0][1][
                "metadata"
            ]["namespace"]
            assert namespace == "foo"

    async def test_allows_namespace_setting_from_manifest(
        self,
        flow_run,
        default_configuration,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod

        default_configuration.job_manifest["metadata"]["namespace"] = "test"
        default_configuration.prepare_for_flow_run(flow_run)

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, default_configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            namespace = mock_batch_client.create_namespaced_job.call_args[0][1][
                "metadata"
            ]["namespace"]
            assert namespace == "test"

    async def test_uses_service_account_name_setting(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod
        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(),
            {"service_account_name": "foo"},
        )

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            service_account_name = mock_batch_client.create_namespaced_job.call_args[0][
                1
            ]["spec"]["template"]["spec"]["serviceAccountName"]
            assert service_account_name == "foo"

    async def test_uses_finished_job_ttl_setting(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod
        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(),
            {"finished_job_ttl": 123},
        )

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            finished_job_ttl = mock_batch_client.create_namespaced_job.call_args[0][1][
                "spec"
            ]["ttlSecondsAfterFinished"]
            assert finished_job_ttl == 123

    async def test_uses_specified_image_pull_policy(
        self,
        flow_run,
        mock_core_client,
        mock_watch,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod
        configuration = await KubernetesWorkerJobConfiguration.from_template_and_values(
            KubernetesWorker.get_default_base_job_template(),
            {"image_pull_policy": "IfNotPresent"},
        )
        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, configuration)
            mock_batch_client.create_namespaced_job.assert_called_once()
            call_image_pull_policy = mock_batch_client.create_namespaced_job.call_args[
                0
            ][1]["spec"]["template"]["spec"]["containers"][0].get("imagePullPolicy")
            assert call_image_pull_policy == "IfNotPresent"

    async def test_defaults_to_incluster_config(
        self,
        flow_run,
        default_configuration,
        mock_core_client,
        mock_watch,
        mock_cluster_config,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, default_configuration)

            mock_cluster_config.load_incluster_config.assert_called_once()
            assert not mock_cluster_config.load_kube_config.called

    async def test_uses_cluster_config_if_not_in_cluster(
        self,
        flow_run,
        default_configuration,
        mock_core_client,
        mock_watch,
        mock_cluster_config,
        mock_batch_client,
    ):
        mock_watch.stream = _mock_pods_stream_that_returns_running_pod
        mock_cluster_config.load_incluster_config.side_effect = ConfigException()
        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, default_configuration)

            mock_cluster_config.new_client_from_config.assert_called_once()

    @pytest.mark.parametrize("job_timeout", [24, 100])
    async def test_allows_configurable_timeouts_for_pod_and_job_watches(
        self,
        mock_core_client,
        mock_watch,
        mock_batch_client,
        job_timeout,
        default_configuration: KubernetesWorkerJobConfiguration,
        flow_run,
    ):
        mock_watch.stream = Mock(side_effect=_mock_pods_stream_that_returns_running_pod)

        # The job should not be completed to start
        mock_batch_client.read_namespaced_job.return_value.status.completion_time = None

        k8s_job_args = dict(
            command=["echo", "hello"],
            pod_watch_timeout_seconds=42,
        )
        expected_job_call_kwargs = dict(
            func=mock_batch_client.list_namespaced_job,
            namespace=mock.ANY,
            field_selector=mock.ANY,
        )

        if job_timeout is not None:
            k8s_job_args["job_watch_timeout_seconds"] = job_timeout
            expected_job_call_kwargs["timeout_seconds"] = pytest.approx(
                job_timeout, abs=1
            )

        default_configuration.job_watch_timeout_seconds = job_timeout
        default_configuration.pod_watch_timeout_seconds = 42

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, default_configuration)

        mock_watch.stream.assert_has_calls(
            [
                mock.call(
                    func=mock_core_client.list_namespaced_pod,
                    namespace=mock.ANY,
                    label_selector=mock.ANY,
                    timeout_seconds=42,
                ),
                mock.call(**expected_job_call_kwargs),
            ]
        )

    @pytest.mark.parametrize("job_timeout", [None])
    async def test_excludes_timeout_from_job_watches_when_null(
        self,
        flow_run,
        default_configuration,
        mock_core_client,
        mock_watch,
        mock_batch_client,
        job_timeout,
    ):
        mock_watch.stream = mock.Mock(
            side_effect=_mock_pods_stream_that_returns_running_pod
        )
        # The job should not be completed to start
        mock_batch_client.read_namespaced_job.return_value.status.completion_time = None

        default_configuration.job_watch_timeout_seconds = job_timeout

        async with KubernetesWorker(work_pool_name="test") as k8s_worker:
            await k8s_worker.run(flow_run, default_configuration)

        mock_watch.stream.assert_has_calls(
            [
                mock.call(
                    func=mock_core_client.list_namespaced_pod,
                    namespace=mock.ANY,
                    label_selector=mock.ANY,
                    timeout_seconds=mock.ANY,
                ),
                mock.call(
                    func=mock_batch_client.list_namespaced_job,
                    namespace=mock.ANY,
                    field_selector=mock.ANY,
                    # Note: timeout_seconds is excluded here
                ),
            ]
        )
