import pytest
from kubernetes.client.exceptions import ApiException
from kubernetes.stream.ws_client import WSClient
from prefect import flow

from prefect_kubernetes.pod import connect_get_namespaced_pod_exec


async def test_connect_get_namespaced_pod_exec_str_return(kubernetes_credentials):
    @flow
    async def test_flow():
        return await connect_get_namespaced_pod_exec(
            pod_name="demo-pod",
            container="app-container",
            command=["whoami"],
            kubernetes_credentials=kubernetes_credentials,
        )

    response = await test_flow()

    assert isinstance(response, str)
    assert response == "root\n"


async def test_connect_get_namespaced_pod_exec_stream_return(kubernetes_credentials):
    @flow
    async def test_flow():
        websocket_client = await connect_get_namespaced_pod_exec(
            pod_name="demo-pod",
            container="app-container",
            command=["/bin/bash"],
            kubernetes_credentials=kubernetes_credentials,
            interactive=True,
        )
        assert isinstance(websocket_client, WSClient)
        assert websocket_client.is_open()

        return websocket_client

    # `test_flow` failing because cannot return a non-pickleable type `SSLSocket` ?
    with pytest.raises(TypeError):
        await test_flow()


async def test_connect_get_namespaced_pod_exec_stream_return_poweruser(
    kubernetes_credentials,
):
    @flow
    async def test_flow():
        websocket_client = await connect_get_namespaced_pod_exec(
            pod_name="demo-pod",
            container="app-container",
            command=["/bin/bash"],
            kubernetes_credentials=kubernetes_credentials,
            _preload_content=False,  # only someone who knows the client well would try this
        )
        assert isinstance(websocket_client, WSClient)
        assert websocket_client.is_open()

    with pytest.raises(TypeError):
        await test_flow()


async def test_connect_get_namespaced_pod_exec_multiline_cmd(kubernetes_credentials):
    @flow
    async def test_flow():
        return await connect_get_namespaced_pod_exec(
            pod_name="demo-pod",
            container="app-container",
            command=["ls", "/"],
            kubernetes_credentials=kubernetes_credentials,
        )

    response = await test_flow()

    assert len(response.splitlines()) == 20


async def test_connect_get_namespaced_pod_exec_no_stdout(kubernetes_credentials):
    @flow
    async def test_flow():
        return await connect_get_namespaced_pod_exec(
            pod_name="demo-pod",
            container="app-container",
            command=["whoami"],
            stdout=False,
            kubernetes_credentials=kubernetes_credentials,
        )

    response = await test_flow()

    assert response == ""


async def test_connect_get_namespaced_pod_exec_fake_pod(kubernetes_credentials):
    @flow
    async def test_flow():
        return await connect_get_namespaced_pod_exec(
            pod_name="non-existent-pod",
            container="app-container",
            command=["whoami"],
            kubernetes_credentials=kubernetes_credentials,
        )

    with pytest.raises(ApiException):
        await test_flow()
