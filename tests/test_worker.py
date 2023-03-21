from contextlib import contextmanager
from unittest.mock import MagicMock
import prefect
import pytest

from time import monotonic

import kubernetes
from kubernetes.config import ConfigException

from prefect.settings import get_current_settings
from prefect.client.schemas import FlowRun
from prefect.server.schemas.core import Flow
from prefect.server.schemas.responses import DeploymentResponse
from prefect.docker import get_prefect_image_name

from prefect_kubernetes import KubernetesWorker
from prefect_kubernetes.utilities import _slugify_label_value
from prefect_kubernetes.worker import KubernetesWorkerJobConfiguration

FAKE_CLUSTER = "fake-cluster"
MOCK_CLUSTER_UID = "1234"


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
    mock.metadata.name = "mock-k8s-v1-job"
    return mock


@pytest.fixture
def mock_core_client(monkeypatch, mock_cluster_config):
    mock = MagicMock(spec=kubernetes.client.CoreV1Api)
    mock.read_namespace.return_value.metadata.uid = MOCK_CLUSTER_UID

    @contextmanager
    def get_client(_):
        yield mock

    monkeypatch.setattr(
        "prefect_kubernetes.worker.KubernetesWorker._get_core_client",
        get_client,
    )
    return mock


@pytest.fixture
def mock_batch_client(monkeypatch, mock_cluster_config, mock_job):
    pytest.importorskip("kubernetes")

    mock = MagicMock(spec=kubernetes.client.BatchV1Api)
    mock.read_namespaced_job.return_value = mock_job
    mock.create_namespaced_job.return_value = mock_job

    @contextmanager
    def get_batch_client(_):
        yield mock

    monkeypatch.setattr(
        "prefect_kubernetes.worker.KubernetesWorker._get_batch_client",
        get_batch_client,
    )
    return mock


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
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "restartPolicy": "Never",
                            "serviceAccountName": "test-service-account",
                            "ttlSecondsAfterFinished": 60,
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
                    }
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
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "restartPolicy": "Never",
                            "serviceAccountName": "test-service-account",
                            "ttlSecondsAfterFinished": 60,
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
                    }
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



