"""Module for interacting with Kubernetes pods from Prefect flows."""
from typing import Any, Dict, Optional

from kubernetes.client.models import V1Pod, V1PodList
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_kubernetes.credentials import KubernetesCredentials


@task
async def create_namespaced_pod(
    body: Dict,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict[str, Any]] = None,
) -> V1Pod:
    """Create a Kubernetes pod in a given namespace."""
    client = kubernetes_credentials.get_core_client()

    return await run_sync_in_worker_thread(
        client.create_namespaced_pod, namespace=namespace, body=body, **kube_kwargs
    )


@task
async def delete_namespaced_pod(
    pod_name: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict[str, Any]] = None,
    delete_option_kwargs: Optional[Dict[str, Any]] = None,
) -> V1Pod:
    """Delete a Kubernetes pod in a given namespace."""
    client = kubernetes_credentials.get_core_client()

    kube_kwargs = kube_kwargs or {}

    if delete_option_kwargs:
        kube_kwargs.update(body=client.V1DeleteOptions(**delete_option_kwargs))

    return await run_sync_in_worker_thread(
        client.delete_namespaced_pod, pod_name, namespace=namespace, **kube_kwargs
    )


@task
async def list_namespaced_pod(
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict[str, Any]] = None,
) -> V1PodList:
    """List all pods in a given namespace."""
    client = kubernetes_credentials.get_core_client()

    return await run_sync_in_worker_thread(
        client.list_namespaced_pod, namespace=namespace, **kube_kwargs
    )


@task
async def patch_namespaced_pod(
    pod_name: str,
    body: Dict,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict[str, Any]] = None,
) -> V1Pod:
    """Patch a Kubernetes pod in a given namespace."""
    client = kubernetes_credentials.get_core_client()

    return await run_sync_in_worker_thread(
        client.patch_namespaced_pod,
        name=pod_name,
        namespace=namespace,
        body=body,
        **kube_kwargs
    )


@task
async def read_namespaced_pod(
    pod_name: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict[str, Any]] = None,
) -> V1Pod:
    """Read information on a Kubernetes pod in a given namespace."""
    client = kubernetes_credentials.get_core_client()

    return await run_sync_in_worker_thread(
        client.read_namespaced_pod, name=pod_name, namespace=namespace, **kube_kwargs
    )


@task
async def read_namespaced_pod_logs(
    pod_name: str,
    container: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
) -> str:
    """Read logs from a Kubernetes pod in a given namespace."""
    client = kubernetes_credentials.get_core_client()

    return await run_sync_in_worker_thread(
        client.read_namespaced_pod_log,
        name=pod_name,
        namespace=namespace,
        container=container,
    )


@task
async def replace_namespaced_pod(
    pod_name: str,
    body: Dict,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict[str, Any]] = None,
) -> V1Pod:
    """Replace a Kubernetes pod in a given namespace."""
    client = kubernetes_credentials.get_core_client()

    return await run_sync_in_worker_thread(
        client.replace_namespaced_pod,
        body=body,
        name=pod_name,
        namespace=namespace,
        **kube_kwargs
    )
