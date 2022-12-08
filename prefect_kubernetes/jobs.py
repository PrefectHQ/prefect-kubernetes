"""Module to define tasks for interacting with Kubernetes jobs."""

from typing import Any, Dict, Optional

from kubernetes.client.models import V1DeleteOptions, V1Job, V1JobList, V1Status
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_kubernetes.credentials import KubernetesCredentials


@task
async def create_namespaced_job(
    kubernetes_credentials: KubernetesCredentials,
    new_job: V1Job,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Job:
    """Task for creating a namespaced Kubernetes job.

    Args:
        kubernetes_credentials: `KubernetesCredentials` block
            holding authentication needed to generate the required API client.
        new_job: A Kubernetes `V1Job` specification.
        namespace: The Kubernetes namespace to create this job in.
        **kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).

    Returns:
        A Kubernetes `V1Job` object.

    Example:
        Create a job in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.jobs import create_namespaced_job
        from kubernetes.client.models import V1Job

        @flow
        def kubernetes_orchestrator():
            v1_job_metadata = create_namespaced_job(
                new_job=V1Job(metadata={"labels": {"foo": "bar"}}),
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
            )
        ```
    """
    with kubernetes_credentials.get_client("batch") as batch_v1_client:

        return await run_sync_in_worker_thread(
            batch_v1_client.create_namespaced_job,
            namespace=namespace,
            body=new_job,
            **kube_kwargs,
        )


@task
async def delete_namespaced_job(
    kubernetes_credentials: KubernetesCredentials,
    job_name: str,
    delete_options: Optional[V1DeleteOptions] = None,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Status:
    """Task for deleting a namespaced Kubernetes job.

    Args:
        kubernetes_credentials: `KubernetesCredentials` block
            holding authentication needed to generate the required API client.
        job_name: The name of a job to delete.
        delete_options: A Kubernetes `V1DeleteOptions` object.
        namespace: The Kubernetes namespace to delete this job in.
        **kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).


    Returns:
        A Kubernetes `V1Status` object.

    Example:
        Delete "my-job" in the default namespace:
        ```python
        from kubernetes.client.models import V1DeleteOptions
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.jobs import delete_namespaced_job

        @flow
        def kubernetes_orchestrator():
            v1_job_status = delete_namespaced_job(
                job_name="my-job",
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
                delete_options=V1DeleteOptions(propagation_policy="Foreground"),
            )
        ```
    """

    with kubernetes_credentials.get_client("batch") as batch_v1_client:

        return await run_sync_in_worker_thread(
            batch_v1_client.delete_namespaced_job,
            name=job_name,
            body=delete_options,
            namespace=namespace,
            **kube_kwargs,
        )


@task
async def list_namespaced_job(
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1JobList:
    """Task for listing namespaced Kubernetes jobs.

    Args:
        kubernetes_credentials: `KubernetesCredentials` block
            holding authentication needed to generate the required API client.
        namespace: The Kubernetes namespace to list jobs from.
        **kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).

    Returns:
        A Kubernetes `V1JobList` object.

    Example:
        List jobs in "my-namespace":
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.jobs import list_namespaced_job

        @flow
        def kubernetes_orchestrator():
            namespaced_job_list = list_namespaced_job(
                namespace="my-namespace",
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
            )
        ```
    """
    with kubernetes_credentials.get_client("batch") as batch_v1_client:

        return await run_sync_in_worker_thread(
            batch_v1_client.list_namespaced_job,
            namespace=namespace,
            **kube_kwargs,
        )


@task
async def patch_namespaced_job(
    kubernetes_credentials: KubernetesCredentials,
    job_name: str,
    job_updates: V1Job,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Job:
    """Task for patching a namespaced Kubernetes job.

    Args:
        kubernetes_credentials: KubernetesCredentials block
            holding authentication needed to generate the required API client.
        job_name: The name of a job to patch.
        job_updates: A Kubernetes `V1Job` specification.
        namespace: The Kubernetes namespace to patch this job in.
        **kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).

    Raises:
        ValueError: if `job_name` is `None`.

    Returns:
        A Kubernetes `V1Job` object.

    Example:
        Patch "my-job" in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.jobs import patch_namespaced_job

        from kubernetes.client.models import V1Job

        @flow
        def kubernetes_orchestrator():
            v1_job_metadata = patch_namespaced_job(
                job_name="my-job",
                job_updates=V1Job(metadata={"labels": {"foo": "bar"}}}),
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
            )
        ```
    """

    with kubernetes_credentials.get_client("batch") as batch_v1_client:

        return await run_sync_in_worker_thread(
            batch_v1_client.patch_namespaced_job,
            name=job_name,
            namespace=namespace,
            body=job_updates,
            **kube_kwargs,
        )


@task
async def read_namespaced_job(
    kubernetes_credentials: KubernetesCredentials,
    job_name: str,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Job:
    """Task for reading a namespaced Kubernetes job.

    Args:
        kubernetes_credentials: `KubernetesCredentials` block
            holding authentication needed to generate the required API client.
        job_name: The name of a job to read.
        namespace: The Kubernetes namespace to read this job in.
        **kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).

    Raises:
        ValueError: if `job_name` is `None`.

    Returns:
        A Kubernetes `V1Job` object.

    Example:
        Read "my-job" in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.jobs import read_namespaced_job

        @flow
        def kubernetes_orchestrator():
            v1_job_metadata = read_namespaced_job(
                job_name="my-job",
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
            )
        ```
    """
    with kubernetes_credentials.get_client("batch") as batch_v1_client:

        return await run_sync_in_worker_thread(
            batch_v1_client.read_namespaced_job,
            name=job_name,
            namespace=namespace,
            **kube_kwargs,
        )


@task
async def replace_namespaced_job(
    kubernetes_credentials: KubernetesCredentials,
    job_name: str,
    new_job: V1Job,
    namespace: Optional[str] = "default",
    **kube_kwargs: Dict[str, Any],
) -> V1Job:
    """Task for replacing a namespaced Kubernetes job.

    Args:
        kubernetes_credentials: `KubernetesCredentials` block
            holding authentication needed to generate the required API client.
        job_name: The name of a job to replace.
        new_job: A Kubernetes `V1Job` specification.
        namespace: The Kubernetes namespace to replace this job in.
        **kube_kwargs: Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`).

    Returns:
        A Kubernetes `V1Job` object.

    Example:
        Replace "my-job" in the default namespace:
        ```python
        from prefect import flow
        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_kubernetes.jobs import replace_namespaced_job

        @flow
        def kubernetes_orchestrator():
            v1_job_metadata = replace_namespaced_job(
                new_job=V1Job(metadata={"labels": {"foo": "bar"}}),
                job_name="my-job",
                kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
            )
        ```
    """
    with kubernetes_credentials.get_client("batch") as batch_v1_client:

        return await run_sync_in_worker_thread(
            batch_v1_client.replace_namespaced_job,
            name=job_name,
            body=new_job,
            namespace=namespace,
            **kube_kwargs,
        )
