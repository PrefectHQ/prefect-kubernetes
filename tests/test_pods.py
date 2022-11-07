import pytest

from prefect_kubernetes.pods import (
    create_namespaced_pod,
    delete_namespaced_pod,
    list_namespaced_pod,
    patch_namespaced_pod,
    read_namespaced_pod,
    read_namespaced_pod_logs,
    replace_namespaced_pod,
)


async def test_create_namespaced_pod(_mock_api_core_client, kubernetes_credentials):
    await create_namespaced_pod.fn()
