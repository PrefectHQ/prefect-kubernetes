from unittest.mock import MagicMock

import pytest

from prefect_kubernetes.credentials import KubernetesApiKey


@pytest.fixture
def successful_job_status():
    job_status = MagicMock()
    job_status.status.active = None
    job_status.status.failed = None
    job_status.status.succeeded = 1
    return job_status


@pytest.fixture
def kubernetes_api_key():
    return KubernetesApiKey(api_key="XXXX")


@pytest.fixture
def api_app_client(monkeypatch):
    app_client = MagicMock()

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesApiKey.get_app_client",
        MagicMock(return_value=app_client),
    )

    return app_client


@pytest.fixture
def api_batch_client(monkeypatch):
    batch_client = MagicMock()

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesApiKey.get_batch_client",
        MagicMock(return_value=batch_client),
    )

    return batch_client


@pytest.fixture
def api_core_client(monkeypatch):
    core_client = MagicMock()

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesApiKey.get_core_client",
        MagicMock(return_value=core_client),
    )

    return core_client


@pytest.fixture
def api_request_method(monkeypatch):
    method = MagicMock()
    method.__self__ = api_core_client

    monkeypatch.setattr(
        "kubernetes.client.api_client.ApiClient.request",
        "method",
        method,
    )
    return method
