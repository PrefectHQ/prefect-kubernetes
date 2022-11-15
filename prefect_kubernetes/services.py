from typing import Any, Dict, Optional

from kubernetes.client.models import V1Service
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_kubernetes.credentials import KubernetesCredentials


@task
async def create_namespaced_service(
    kubernetes_credentials: KubernetesCredentials,
    new_service: V1Service,
    namespace: Optional[str] = "default",
    **kube_kwargs: Optional[Dict[str, Any]],
) -> V1Service:
    """Create a namespaced Kubernetes service.

    Args:
        kubernetes_credentials: A `KubernetesCredentials` block used to generate a
            `CoreV1Api` client.
        new_service: A `V1Service` object representing the service to create.
        namespace: The namespace to create the service in.
        **kube_kwargs: Additional keyword arguments to pass to the `CoreV1Api`
            method call.

    Returns:
        A `V1Service` object.

    Example:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.services import create_namespaced_service
        from kubernetes.client.models import V1Service

        @flow
        def create_service_flow():
            v1_service = create_namespaced_service(
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
                new_service=V1Service(metadata={...}, spec={...}),
            )
        ```
    """
    with kubernetes_credentials.get_client("core") as core_v1_client:

        return await run_sync_in_worker_thread(
            core_v1_client.create_namespaced_service,
            body=new_service,
            namespace=namespace,
            **kube_kwargs,
        )
