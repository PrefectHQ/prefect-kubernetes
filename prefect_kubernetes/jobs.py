"""Module to define tasks for interacting with Kubernetes jobs."""

from typing import Dict, Optional

from kubernetes import client
from kubernetes.client.models import V1Job, V1JobList, V1Status
from prefect import task

from prefect_kubernetes.credentials import KubernetesCredentials


@task
async def create_namespaced_job(
    body: Dict,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = None,
) -> V1Job:
    """Task for creating a namespaced Kubernetes job.

    Args:
        body: A dictionary representing a Kubernetes V1Job specification.
        namespace: The Kubernetes namespace to create this job in.
        kubernetes_credentials: KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).


    Returns:
        A Kubernetes `V1Job` object.
    """
    api_client = kubernetes_credentials.get_batch_client()

    return api_client.create_namespaced_job(
        namespace=namespace, body=body, **kube_kwargs
    )


@task
async def delete_namespaced_job(
    job_name: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = None,
    delete_option_kwargs: Optional[Dict] = None,
) -> V1Status:
    """Task for deleting a namespaced Kubernetes job.

    Args:
        job_name: The name of a job to delete.
        namespace: The Kubernetes namespace to delete this job in.
        kubernetes_credentials: KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).
        - delete_option_kwargs: Optional keyword arguments to pass to
            the V1DeleteOptions object (e.g. {"propagation_policy": "...",
            "grace_period_seconds": "..."}.

    Returns:
        A Kubernetes `V1Status` object.
    """

    api_client = kubernetes_credentials.get_batch_client()

    kube_kwargs = kube_kwargs or {}

    if delete_option_kwargs:
        kube_kwargs.update(body=client.V1DeleteOptions(**delete_option_kwargs))

    return api_client.delete_namespaced_job(
        name=job_name, namespace=namespace, **kube_kwargs
    )


@task
async def list_namespaced_job(
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = None,
) -> V1JobList:
    """Task for listing namespaced Kubernetes jobs.

    Args:
        kubernetes_credentials: KubernetesCredentials block
            holding authentication needed to generate the required API client.
        namespace: The Kubernetes namespace to list jobs from.
        kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).

    Returns:
        A Kubernetes `V1JobList` object.
    """
    api_client = kubernetes_credentials.get_batch_client()

    kube_kwargs = kube_kwargs or {}

    return api_client.list_namespaced_job(
        namespace=namespace,
        **kube_kwargs,
    )


@task
async def patch_namespaced_job(
    job_name: str,
    body: Dict,
    kubernetes_credentials: KubernetesCredentials = None,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = None,
) -> V1Job:
    """Task for deleting a namespaced Kubernetes job.

    Args:
        job_name: The name of a job to patch.
        body: A dictionary representation of a Kubernetes V1Job
            specification.
        namespace: The Kubernetes namespace to patch this job in.
        kubernetes_credentials: KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).

    Raises:
        ValueError: if `job_name` is `None`

    Returns:
        A Kubernetes `V1Job` object.
    """

    api_client = kubernetes_credentials.get_batch_client()

    kube_kwargs = kube_kwargs or {}

    return api_client.patch_namespaced_job(
        name=job_name, namespace=namespace, body=body, **kube_kwargs
    )


@task
async def read_namespaced_job(
    job_name: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = None,
) -> V1Job:
    """Task for reading a namespaced Kubernetes job.

    Args:
        job_name: The name of a job to read.
        namespace: The Kubernetes namespace to read this job in.
        kubernetes_credentials: KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).

    Raises:
        ValueError: if `job_name` is `None`.

    Returns:
        A Kubernetes `V1Job` object.
    """
    api_client = kubernetes_credentials.get_batch_client()

    kube_kwargs = kube_kwargs or {}

    return api_client.read_namespaced_job(
        name=job_name, namespace=namespace, **kube_kwargs
    )


@task
async def replace_namespaced_job(
    body: dict,
    job_name: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = None,
) -> V1Job:
    """Task for replacing a namespaced Kubernetes job.

    Args:
        body: A dictionary representation of a Kubernetes V1Job
            specification
        job_name: The name of a job to replace.
        namespace: The Kubernetes namespace to replace this job in.
        kubernetes_credentials: KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).

    Returns:
        A Kubernetes `V1Job` object.
    """
    api_client = kubernetes_credentials.get_batch_client()

    kube_kwargs = kube_kwargs or {}

    return api_client.replace_namespaced_job(
        name=job_name, body=body, namespace=namespace, **kube_kwargs
    )
