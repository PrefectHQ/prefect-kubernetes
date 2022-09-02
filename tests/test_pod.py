import pytest

from prefect_kubernetes.pod import connect_get_namespaced_pod_exec


async def test_connect_get_namespaced_pod_exec(kubernetes_api_key):

    response = await connect_get_namespaced_pod_exec.fn(
        pod_name="test_pod",
        container_name="test_container",
        exec_command=["whoami"],
        namespace="test_ns",
        kubernetes_api_key=kubernetes_api_key,
        kube_kwargs={},
    )

    assert 1 == response.splitlines()
