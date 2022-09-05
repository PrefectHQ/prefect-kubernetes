from urllib import response

import pytest
from kubernetes.client.exceptions import ApiException
from kubernetes.stream.ws_client import WSClient
from prefect import flow
from prefect.orion.schemas.states import Completed

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
        response = await connect_get_namespaced_pod_exec(
            pod_name="demo-pod",
            container="app-container",
            command=["/bin/bash"],
            kubernetes_credentials=kubernetes_credentials,
            _preload_content=False,
        )
        # `test_flow` failing because cannot `return` non-pickleable type `SSLSocket` ?
        assert isinstance(response, WSClient)
        return None

    try:
        await test_flow()
    except TypeError as e:
        if "cannot pickle 'SSLSocket' object" in str(e):
            pass
        else:
            raise


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
            stdout=False,
            kubernetes_credentials=kubernetes_credentials,
        )

    with pytest.raises(ApiException):
        await test_flow()
