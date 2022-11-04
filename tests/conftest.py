from pathlib import Path
from unittest import mock

import pytest
import yaml
from prefect.blocks.kubernetes import KubernetesClusterConfig

from prefect_kubernetes.credentials import KubernetesCredentials

BASEDIR = Path("tests")
GOOD_CONFIG_FILE_PATH = BASEDIR / "kube_config.yaml"


@pytest.fixture
def kube_config_dict():
    return yaml.safe_load(GOOD_CONFIG_FILE_PATH.read_text())


@pytest.fixture
def successful_job_status():
    job_status = mock.MagicMock()
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
    app_client = mock.MagicMock()

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_app_client",
        mock.MagicMock(return_value=app_client),
    )

    return app_client


@pytest.fixture
def _mock_api_batch_client(monkeypatch):
    batch_client = mock.MagicMock()

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_batch_client",
        mock.MagicMock(return_value=batch_client),
    )

    return batch_client


@pytest.fixture
def _mock_api_core_client(monkeypatch):
    core_client = mock.MagicMock()

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_core_client",
        mock.MagicMock(return_value=core_client),
    )

    return core_client
