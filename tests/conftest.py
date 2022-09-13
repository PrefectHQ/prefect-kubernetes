from unittest import mock

import pytest


@pytest.fixture
def api_app_client(monkeypatch):
    app_client = mock.MagicMock()

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_app_client",
        mock.MagicMock(return_value=app_client),
    )

    return app_client


@pytest.fixture
def api_batch_client(monkeypatch):
    batch_client = mock.MagicMock()

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_batch_client",
        mock.MagicMock(return_value=batch_client),
    )

    return batch_client


@pytest.fixture
def api_core_client(monkeypatch):
    core_client = mock.MagicMock()

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_core_client",
        mock.MagicMock(return_value=core_client),
    )

    return core_client
