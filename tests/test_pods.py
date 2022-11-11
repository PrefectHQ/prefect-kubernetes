import pytest
from kubernetes.client.exceptions import ApiValueError
from kubernetes.client.models import V1DeleteOptions, V1Pod

from prefect_kubernetes.pods import (
    create_namespaced_pod,
    delete_namespaced_pod,
    list_namespaced_pod,
    patch_namespaced_pod,
    read_namespaced_pod,
    read_namespaced_pod_logs,
    replace_namespaced_pod,
)


async def test_invalid_body_raises_error(kubernetes_credentials):
    with pytest.raises(ApiValueError):
        await create_namespaced_pod.fn(
            body=None, kubernetes_credentials=kubernetes_credentials
        )
    with pytest.raises(ApiValueError):
        await patch_namespaced_pod.fn(
            body=None, pod_name="", kubernetes_credentials=kubernetes_credentials
        )


async def test_create_namespaced_pod(kubernetes_credentials, _mock_api_core_client):
    await create_namespaced_pod.fn(
        body=V1Pod(**{"metadata": {"name": "test-pod"}}),
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )

    assert _mock_api_core_client.create_namespaced_pod.call_args[1][
        "body"
    ].metadata == {"name": "test-pod"}
    assert _mock_api_core_client.create_namespaced_pod.call_args[1]["a"] == "test"


async def test_delete_namespaced_pod(kubernetes_credentials, _mock_api_core_client):
    await delete_namespaced_pod.fn(
        kubernetes_credentials=kubernetes_credentials,
        pod_name="test_pod",
        body=V1DeleteOptions(grace_period_seconds=42),
        a="test",
    )
    assert (
        _mock_api_core_client.delete_namespaced_pod.call_args[1]["namespace"]
        == "default"
    )
    assert _mock_api_core_client.delete_namespaced_pod.call_args[1]["a"] == "test"
    assert (
        _mock_api_core_client.delete_namespaced_pod.call_args[1][
            "body"
        ].grace_period_seconds
        == 42
    )


async def test_bad_v1_delete_options(kubernetes_credentials, _mock_api_core_client):
    with pytest.raises(TypeError):
        await delete_namespaced_pod.fn(
            kubernetes_credentials=kubernetes_credentials,
            pod_name="test_pod",
            body=V1DeleteOptions(skrrrt_skrrrt="yeehaw"),
        )


async def test_list_namespaced_pod(kubernetes_credentials, _mock_api_core_client):
    await list_namespaced_pod.fn(
        namespace="ns",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert _mock_api_core_client.list_namespaced_pod.call_args[1]["namespace"] == "ns"
    assert _mock_api_core_client.list_namespaced_pod.call_args[1]["a"] == "test"


async def test_patch_namespaced_pod(kubernetes_credentials, _mock_api_core_client):
    await patch_namespaced_pod.fn(
        body=V1Pod(**{"metadata": {"name": "test-pod"}}),
        pod_name="test_pod",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert _mock_api_core_client.patch_namespaced_pod.call_args[1]["body"].metadata == {
        "name": "test-pod"
    }
    assert _mock_api_core_client.patch_namespaced_pod.call_args[1]["name"] == "test_pod"
    assert _mock_api_core_client.patch_namespaced_pod.call_args[1]["a"] == "test"


async def test_read_namespaced_pod(kubernetes_credentials, _mock_api_core_client):
    await read_namespaced_pod.fn(
        pod_name="test_pod",
        namespace="ns",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert _mock_api_core_client.read_namespaced_pod.call_args[1]["name"] == "test_pod"
    assert _mock_api_core_client.read_namespaced_pod.call_args[1]["namespace"] == "ns"
    assert _mock_api_core_client.read_namespaced_pod.call_args[1]["a"] == "test"


async def test_read_namespaced_pod_logs(kubernetes_credentials, _mock_api_core_client):
    await read_namespaced_pod_logs.fn(
        pod_name="test_pod",
        container="test_container",
        namespace="ns",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert (
        _mock_api_core_client.read_namespaced_pod_log.call_args[1]["name"] == "test_pod"
    )
    assert (
        _mock_api_core_client.read_namespaced_pod_log.call_args[1]["namespace"] == "ns"
    )
    assert (
        _mock_api_core_client.read_namespaced_pod_log.call_args[1]["container"]
        == "test_container"
    )
    assert _mock_api_core_client.read_namespaced_pod_log.call_args[1]["a"] == "test"


async def test_replace_namespaced_pod(kubernetes_credentials, _mock_api_core_client):
    await replace_namespaced_pod.fn(
        pod_name="test_pod",
        body=V1Pod(**{"metadata": {"name": "test-pod"}}),
        namespace="ns",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert (
        _mock_api_core_client.replace_namespaced_pod.call_args[1]["name"] == "test_pod"
    )
    assert (
        _mock_api_core_client.replace_namespaced_pod.call_args[1]["namespace"] == "ns"
    )
    assert _mock_api_core_client.replace_namespaced_pod.call_args[1][
        "body"
    ].metadata == {"name": "test-pod"}
    assert _mock_api_core_client.replace_namespaced_pod.call_args[1]["a"] == "test"


@pytest.mark.parametrize(
    "task_accepting_pod",
    [create_namespaced_pod, patch_namespaced_pod, replace_namespaced_pod],
)
async def test_bad_v1_pod_kwargs(kubernetes_credentials, task_accepting_pod):
    with pytest.raises(TypeError):
        await task_accepting_pod.fn(
            body=V1Pod(**{"random_not_real": "shabba-ranks"}),
            kubernetes_credentials=kubernetes_credentials,
        )
