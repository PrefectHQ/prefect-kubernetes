from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml
from kubernetes.client import AppsV1Api, BatchV1Api, CoreV1Api, models
from kubernetes.client.exceptions import ApiException
from prefect.blocks.kubernetes import KubernetesClusterConfig

from prefect_kubernetes.credentials import KubernetesCredentials

BASEDIR = Path("tests")
GOOD_CONFIG_FILE_PATH = BASEDIR / "kube_config.yaml"


@pytest.fixture
def kube_config_dict():
    return yaml.safe_load(GOOD_CONFIG_FILE_PATH.read_text())


@pytest.fixture
def successful_job_status():
    job_status = MagicMock()
    job_status.status.active = None
    job_status.status.failed = None
    job_status.status.succeeded = 1
    return job_status


@pytest.fixture
def kubernetes_credentials(kube_config_dict):
    return KubernetesCredentials(
        cluster_config=KubernetesClusterConfig(
            context_name="test", config=kube_config_dict
        )
    )


@pytest.fixture
def _mock_api_app_client(monkeypatch):
    app_client = MagicMock(spec=AppsV1Api)

    @contextmanager
    def get_client(self, _):
        yield app_client

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_client",
        get_client,
    )

    return app_client


@pytest.fixture
def _mock_api_batch_client(monkeypatch):
    batch_client = MagicMock(spec=BatchV1Api)

    @contextmanager
    def get_client(self, _):
        yield batch_client

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_client",
        get_client,
    )

    return batch_client


@pytest.fixture
def _mock_api_core_client(monkeypatch):
    core_client = MagicMock(spec=CoreV1Api)

    @contextmanager
    def get_client(self, _):
        yield core_client

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_client",
        get_client,
    )

    return core_client


@pytest.fixture
def mock_stream_timeout(monkeypatch):

    monkeypatch.setattr(
        "kubernetes.watch.Watch.stream",
        MagicMock(side_effect=ApiException(status=408)),
    )


@pytest.fixture
def mock_pod_log(monkeypatch):
    monkeypatch.setattr(
        "kubernetes.watch.Watch.stream",
        MagicMock(return_value=["test log"]),
    )


@pytest.fixture
def mock_list_namespaced_pod(monkeypatch):

    mock_pods = AsyncMock(
        return_value=models.V1PodList(
            items=[
                models.V1Pod(
                    metadata=models.V1ObjectMeta(name="test-pod"),
                    status=models.V1PodStatus(phase="Completed"),
                )
            ]
        )
    )

    monkeypatch.setattr("prefect_kubernetes.pods.list_namespaced_pod.fn", mock_pods)


@pytest.fixture
def read_pod_logs(monkeypatch):
    read_pod_logs = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "prefect_kubernetes.pods.read_namespaced_pod_log.fn", read_pod_logs
    )
    return read_pod_logs
