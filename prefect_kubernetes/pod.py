import asyncio
from typing import Callable, List, Optional, Union

from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.stream.ws_client import WSClient
from kubernetes.watch import Watch
from prefect import get_run_logger, task

from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.exceptions import KubernetesResourceNotFoundError

KUBERNETES_RESOURCE_NOT_FOUND_STATUS_CODE = 0


@task
async def read_namespaced_pod_logs(
    pod_name: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: str = "default",
    on_log_entry: Callable = None,
    container: str = None,
) -> None:
    """
    Task run method.
    Args:
        - pod_name (str, optional): The name of a pod to replace
        - kubernetes_credentials (KubernetesCredentials): name of the KubernetesCredentials block
        - namespace (str, optional): The Kubernetes namespace to read pod logs in,
            defaults to the `default` namespace
        - on_log_entry (Callable, optional): If provided, will stream the pod logs
            calling the callback for every line (and the task returns `None`). If not
            provided, the current pod logs will be returned immediately from the task.
        - container (str, optional): The name of the container to read logs from
    """

    api_core_client = kubernetes_credentials.get_core_client()

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


@task
async def connect_get_namespaced_pod_exec(
    name: str,
    container: str,
    command: List[str],
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    **kwargs,
) -> Union[str, WSClient]:
    """Task for running and/or streaming commands in a namespaced pod on Kubernetes.

    This task requires `KubernetesCredentials` to generate a`CoreV1Api` Kubernetes
    client to run / stream commands on the specified pod's `container`.

    User-provided `kwargs` will overwrite `default_kwargs` if keys exist in both.

    The `kubernetes.stream.stream` object accepts a `_preload_content` kwarg (defualts to True) which
    determines this task's return value type.

    If `_preload_content=True`, `api_response` will be the `str` output of `command` on `container`.
    Otherwise if `_preload_content=False`, `api_response` will be an interactive `WSClient` object.

    Note that since `WSClient` is a non-pickleable object-type, it cannot be used as the `return` value
    of a @flow-decorated function definition.

    Args:
        name (str): The name of the pod in which the command is to be run
        container (str): The name of a container to use in the pod.
        command (List): The command to run in `pod_name`
        kubernetes_credentials (KubernetesCredentials): A block that stores a Kubernetes credentials,
            has methods to generate resource-specific client
        namespace (str, optional): The Kubernetes namespace of the pod.
            Defaults to `default`
        kwargs (Dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API method (e.g. `{"stderr": "False", "tty": "True"}`)

    Returns:
        Union[str, WSClient]: This task either returns the `str` output of `command`, or if
            `_preload_content=False`, then an interactive `WSClient` object is returned.

    Raises:
        - TypeError: `command` is not a list, or `api_response` is of unexpected type
        - KubernetesResourceNotFoundError: if `api_response` has KUBERNETES_RESOURCE_NOT_FOUND_STATUS_CODE
        - ApiException: if bad `api_response` status and is not KUBERNETES_RESOURCE_NOT_FOUND_STATUS_CODE
    """

    logger = get_run_logger()

    if not isinstance(command, List):
        raise TypeError("The `command` argument must be provided as a list")

    api_client = kubernetes_credentials.get_core_client()

    default_kwargs = dict(
        stderr=True,
        stdin=True,
        stdout=True,
        tty=False,
    )

    method_kwargs = {**default_kwargs, **kwargs}

    try:
        api_response = stream(
            api_client.connect_get_namespaced_pod_exec,
            name=name,
            namespace=namespace,
            container=container,
            command=command,
            **method_kwargs,
        )

        if isinstance(api_response, str):
            logger.info(
                f"Returning `str` output of '{' '.join(command)}' as executed on {container}..."
            )
        elif isinstance(api_response, WSClient):
            logger.info(
                f"Returning an interactive `kubernetes.stream.ws_client.WSClient` object..."
            )
        else:
            raise TypeError(
                f"Unexpected API response object-type: {type(api_response)}"
            )

        return api_response

    except ApiException as err:
        if err.status == KUBERNETES_RESOURCE_NOT_FOUND_STATUS_CODE:
            raise KubernetesResourceNotFoundError(
                status=404,
                reason=(
                    f"{err.reason}"
                    " - Your Kubernetes API client cannot find a resource you specified."
                ),
            )
        else:
            logger.error(f"{err.reason}")
            raise
