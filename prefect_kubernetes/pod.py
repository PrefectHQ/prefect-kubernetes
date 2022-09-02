from typing import Any, Callable, Dict, List

from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.watch import Watch
from prefect import task
from prefect_kubernetes.credentials import KubernetesApiKey


@task
async def read_namespaced_pod_logs(
    pod_name: str = None,
    namespace: str = "default",
    on_log_entry: Callable = None,
    kubernetes_api_key: KubernetesApiKey = None,
    container: str = None,
) -> None:
    """
    Task run method.
    Args:
        - pod_name (str, optional): The name of a pod to replace
        - namespace (str, optional): The Kubernetes namespace to read pod logs in,
            defaults to the `default` namespace
        - on_log_entry (Callable, optional): If provided, will stream the pod logs
            calling the callback for every line (and the task returns `None`). If not
            provided, the current pod logs will be returned immediately from the task.
        - kubernetes_api_key (KubernetesApiKey, optional): name of the KubernetesApiKey block
            containing your Kubernetes API Key; value must be a string in BearerToken format
        - container (str, optional): The name of the container to read logs from
    Raises:
        - ValueError: if `pod_name` is `None`
    """
    if not pod_name:
        raise ValueError("The name of a Kubernetes pod must be provided.")

    if kubernetes_api_key:
        api_core_client = kubernetes_api_key.get_core_client()
    else:
        raise ValueError("A `KubernetesApiKey` block must be provided")

    if on_log_entry is None:
        return api_core_client.read_namespaced_pod_log(
            name=pod_name, namespace=namespace, container=container
        )

    # From the kubernetes.watch documentation:
    # Note that watching an API resource can expire. The method tries to
    # resume automatically once from the last result, but if that last result
    # is too old as well, an `ApiException` exception will be thrown with
    # ``code`` 410.
    while True:
        try:
            stream = Watch().stream(
                api_core_client.read_namespaced_pod_log,
                name=pod_name,
                namespace=namespace,
                container=container,
            )

            for log in stream:
                on_log_entry(log)

            return
        except ApiException as exception:
            if exception.status != 410:
                raise


@task(name="Run a command in a namespaced pod on Kubernetes")
async def connect_get_namespaced_pod_exec(
    pod_name: str = None,
    container_name: str = None,
    exec_command: List = None,
    kubernetes_api_key: KubernetesApiKey = None,
    namespace: str = "default",
    kube_kwargs: Dict = {},
) -> Callable:
    """Task for running a command in a namespaced pod on Kubernetes.

    This task requires a `KubernetesApiKey` to generate a`CoreV1Api` Kubernetes
    client to stream the overridden `api_response` to `connect_get_namespaced_pod_exec`.

    Args:
        pod_name (str): The name of a pod in which the command is to be run
        container_name (str): The name of a container to use in the pod.
            Defaults to `None`
        exec_command (List): The command to run in `pod_name`
            Defaults to `None`
        kubernetes_api_key (KubernetesApiKey): A block that stores a Kubernetes api,
            has methods to generate resource specific client
        namespace (str, optional): The Kubernetes namespace of the pod.
                Defaults to `default`
        kube_kwargs (Dict, optional): Optional extra keyword arguments to pass to the
                Kubernetes API (e.g. `{"pretty": "...", "exact": "..."}`)

    Returns:
        Callable: A Kubernetes websocket `stream` according to supplied Kubernetes API method
            and kwarg overrides


    Raises:
        - ValueError: if `pod_name` is `None` or `container_name` is `None`
        - TypeError: `exec_command` is not a list
    """
    if not (pod_name and container_name):
        raise ValueError("The name of a Kubernetes pod and container must be provided.")

    if not isinstance(exec_command, List):
        raise TypeError("The `exec_command` argument must be provided as a list")

    if not kubernetes_api_key:
        raise ValueError("An existing `KubernetesApiKey` block must be provided.")

    api_client = kubernetes_api_key.get_core_client()

    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    api_response = stream(
        api_client.connect_get_namespaced_pod_exec,
        name=pod_name,
        namespace=namespace,
        container=container_name,
        command=exec_command,
        stderr=True,
        stdin=True,
        stdout=True,
        tty=False,
        **kube_kwargs
    )

    return api_response
