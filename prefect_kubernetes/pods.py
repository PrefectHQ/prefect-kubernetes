"""Module for interacting with Kubernetes pods from Prefect flows."""
from typing import Any, Dict, Optional

from kubernetes.client.models import V1DeleteOptions, V1Pod, V1PodList
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_kubernetes.credentials import KubernetesCredentials


@task
async def create_namespaced_pod(
    kubernetes_credentials: KubernetesCredentials,
    body: Dict,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Pod:
    """Create a Kubernetes pod in a given namespace.
    
    Args:
        kubernetes_credentials: `KubernetesCredentials` block for creating
            authenticated Kubernetes API clients.
        body: A Kubernetes `V1Pod` specification.
        namespace: The Kubernetes namespace to create this pod in.
        **kube_kwargs: Optional extra keyword arguments to pass to the Kubernetes API.

    Returns:
        A Kubernetes `V1Pod` object.

    Example:
        Create a pod in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.pods import create_namespaced_pod
        from kubernetes.client.models import V1Pod
        
        @flow
        def kubernetes_orchestrator():
            v1_pod_metadata = create_namespaced_pod(
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
                body=V1Pod(**{"metadata": {"name": "test-pod"}}),
            )
        ```
    """
    with kubernetes_credentials.get_core_client() as api_client:

        return await run_sync_in_worker_thread(
            api_client.create_namespaced_pod,
            namespace=namespace,
            body=body,
            **kube_kwargs,
        )


@task
async def delete_namespaced_pod(
    kubernetes_credentials: KubernetesCredentials,
    pod_name: str,
    body: Optional[V1DeleteOptions] = None,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Pod:
    """Delete a Kubernetes pod in a given namespace.
    
    Args:
        kubernetes_credentials: `KubernetesCredentials` block for creating
            authenticated Kubernetes API clients.
        pod_name: The name of the pod to delete.
        body: A Kubernetes `V1DeleteOptions` object.
        namespace: The Kubernetes namespace to delete this pod from.
        **kube_kwargs: Optional extra keyword arguments to pass to the Kubernetes API.
    
    Returns:
        A Kubernetes `V1Pod` object.
        
    Example:
        Delete a pod in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.pods import delete_namespaced_pod
        from kubernetes.client.models import V1DeleteOptions
        
        @flow
        def kubernetes_orchestrator():
            v1_pod_metadata = delete_namespaced_pod(
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
                pod_name="test-pod",
                body=V1DeleteOptions(grace_period_seconds=0),
            )
        ```
    """
    with kubernetes_credentials.get_core_client() as api_client:

        return await run_sync_in_worker_thread(
            api_client.delete_namespaced_pod,
            pod_name,
            body=body,
            namespace=namespace,
            **kube_kwargs,
        )


@task
async def list_namespaced_pod(
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1PodList:
    """List all pods in a given namespace.
    
    Args:
        kubernetes_credentials: `KubernetesCredentials` block for creating
            authenticated Kubernetes API clients.
        namespace: The Kubernetes namespace to list pods from.
        **kube_kwargs: Optional extra keyword arguments to pass to the Kubernetes API.
        
    Returns:
        A Kubernetes `V1PodList` object.
    
    Example:
        List all pods in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.pods import list_namespaced_pod
        
        @flow
        def kubernetes_orchestrator():
            v1_pod_list = list_namespaced_pod(
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds")
            )
        ```
    """
    with kubernetes_credentials.get_core_client() as api_client:

        return await run_sync_in_worker_thread(
            api_client.list_namespaced_pod, namespace=namespace, **kube_kwargs
        )


@task
async def patch_namespaced_pod(
    kubernetes_credentials: KubernetesCredentials,
    pod_name: str,
    body: V1Pod,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Pod:
    """Patch a Kubernetes pod in a given namespace.
    
    Args:
        kubernetes_credentials: `KubernetesCredentials` block for creating
            authenticated Kubernetes API clients.
        pod_name: The name of the pod to patch.
        body: A Kubernetes `V1Pod` object.
        namespace: The Kubernetes namespace to patch this pod in.
        **kube_kwargs: Optional extra keyword arguments to pass to the Kubernetes API.
        
    Returns:
        A Kubernetes `V1Pod` object.
    
    Example:
        Patch a pod in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.pods import patch_namespaced_pod
        from kubernetes.client.models import V1Pod
        
        @flow
        def kubernetes_orchestrator():
            v1_pod_metadata = patch_namespaced_pod(
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
                pod_name="test-pod",
                body=V1Pod(**{"metadata": {"labels": {"foo": "bar"}}}),
            )
        ```
    """
    with kubernetes_credentials.get_core_client() as api_client:

        return await run_sync_in_worker_thread(
            api_client.patch_namespaced_pod,
            name=pod_name,
            namespace=namespace,
            body=body,
            **kube_kwargs,
        )


@task
async def read_namespaced_pod(
    kubernetes_credentials: KubernetesCredentials,
    pod_name: str,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Pod:
    """Read information on a Kubernetes pod in a given namespace.
    
    Args:
        kubernetes_credentials: `KubernetesCredentials` block for creating
            authenticated Kubernetes API clients.
        pod_name: The name of the pod to read.
        namespace: The Kubernetes namespace to read this pod from.
        **kube_kwargs: Optional extra keyword arguments to pass to the Kubernetes API.
        
    Returns:
        A Kubernetes `V1Pod` object.
    
    Example:
        Read a pod in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        
        @flow
        def kubernetes_orchestrator():
            v1_pod_metadata = read_namespaced_pod(
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
                pod_name="test-pod"
            )
        ```
    """
    with kubernetes_credentials.get_core_client() as api_client:

        return await run_sync_in_worker_thread(
            api_client.read_namespaced_pod,
            name=pod_name,
            namespace=namespace,
            **kube_kwargs,
        )


@task
async def read_namespaced_pod_logs(
    kubernetes_credentials: KubernetesCredentials,
    pod_name: str,
    container: str,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> str:
    """Read logs from a Kubernetes pod in a given namespace.
    
    Args:
        kubernetes_credentials: `KubernetesCredentials` block for creating
            authenticated Kubernetes API clients.
        pod_name: The name of the pod to read logs from.
        container: The name of the container to read logs from.
        namespace: The Kubernetes namespace to read this pod from.
        **kube_kwargs: Optional extra keyword arguments to pass to the Kubernetes API.
        
    Returns:
        A string containing the logs from the pod's container.
        
    Example:
        Read logs from a pod in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.pods import read_namespaced_pod_logs
        
        @flow
        def kubernetes_orchestrator():
            pod_logs = read_namespaced_pod_logs(
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
                pod_name="test-pod",
                container="test-container"
            )
        ```
    """
    with kubernetes_credentials.get_core_client() as api_client:

        return await run_sync_in_worker_thread(
            api_client.read_namespaced_pod_log,
            name=pod_name,
            namespace=namespace,
            container=container,
            **kube_kwargs,
        )


@task
async def replace_namespaced_pod(
    kubernetes_credentials: KubernetesCredentials,
    pod_name: str,
    body: V1Pod,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Pod:
    """Replace a Kubernetes pod in a given namespace.
    
    Args:
        kubernetes_credentials: `KubernetesCredentials` block for creating
            authenticated Kubernetes API clients.
        pod_name: The name of the pod to replace.
        body: A Kubernetes `V1Pod` object.
        namespace: The Kubernetes namespace to replace this pod in.
        **kube_kwargs: Optional extra keyword arguments to pass to the Kubernetes API.
        
    Returns:
        A Kubernetes `V1Pod` object.
    
    Example:
        Replace a pod in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.pods import replace_namespaced_pod
        from kubernetes.client.models import V1Pod
        
        @flow
        def kubernetes_orchestrator():
            v1_pod_metadata = replace_namespaced_pod(
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
                pod_name="test-pod",
                body=V1Pod(**{"metadata": {"labels": {"foo": "bar"}}})
            )
        ```
    """
    with kubernetes_credentials.get_core_client() as api_client:

        return await run_sync_in_worker_thread(
            api_client.replace_namespaced_pod,
            body=body,
            name=pod_name,
            namespace=namespace,
            **kube_kwargs,
        )
