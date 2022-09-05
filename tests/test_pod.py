from prefect_kubernetes.pod import connect_get_namespaced_pod_exec


async def test_connect_get_namespaced_pod_exec(kubernetes_credentials, mock_ApiClient):

    response = await connect_get_namespaced_pod_exec.fn(
        name="demo-pod",
        container="app-container",
        command=["whoami"],
        kubernetes_credentials=kubernetes_credentials,
    )

    assert response == "root\n"


async def test_connect_get_namespaced_pod_exec_multiline_cmd(
    kubernetes_credentials, mock_ApiClient
):

    response = await connect_get_namespaced_pod_exec.fn(
        name="demo-pod",
        container="app-container",
        command=["ls", "/"],
        kubernetes_credentials=kubernetes_credentials,
    )
    assert len(response.splitlines()) == 20


async def test_connect_get_namespaced_pod_exec_no_stdout(
    kubernetes_credentials, mock_ApiClient
):

    response = await connect_get_namespaced_pod_exec.fn(
        name="demo-pod",
        container="app-container",
        command=["whoami"],
        stdout=False,
        kubernetes_credentials=kubernetes_credentials,
    )

    assert response == ""
