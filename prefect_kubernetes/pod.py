from typing import Callable

from kubernetes.watch import Watch
from kubernetes.client.rest import ApiException

from prefect import task
from prefect_kubernetes.credentials import KubernetesApiKey


@task
def read_namespaced_pod_logs(
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

    api_core_client = kubernetes_api_key.get_core_client()

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
