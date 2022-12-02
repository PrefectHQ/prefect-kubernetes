"""Module to define tasks for interacting with Kubernetes jobs."""

from asyncio import sleep
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from kubernetes.client.models import V1DeleteOptions, V1Job, V1JobList, V1Status
from prefect import get_run_logger, task
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from typing_extensions import Literal

from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.exceptions import KubernetesJobFailedError
from prefect_kubernetes.pods import list_namespaced_pod, read_namespaced_pod_log
from prefect_kubernetes.utilities import convert_manifest_to_model


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


@task
async def run_namespaced_job(
    kubernetes_credentials: KubernetesCredentials,
    job_to_run: Union[V1Job, Dict[str, Any], Path, str],
    namespace: Optional[str] = "default",
    job_status_poll_interval: Optional[int] = 5,
    log_level: Optional[
        Literal["INFO", "DEBUG", "ERROR", "WARN", "CRITICAL", None]
    ] = None,
    delete_job_after_completion: Optional[bool] = True,
) -> Tuple[V1Job, Dict[str, str]]:
    """Task for running a namespaced Kubernetes job.

    Args:
        kubernetes_credentials: `KubernetesCredentials` block
            holding authentication needed to generate the required API client.
        job_to_run: A Kubernetes `V1Job` specification.
        namespace: The Kubernetes namespace to run this job in.
        job_status_poll_interval: The number of seconds to wait between job status
            checks.
        log_level: The log level to use when outputting job logs. If `None`, logs
            from the job will not be captured.
        delete_job_after_completion: Whether to delete the job after it has completed.

    Returns:
        A tuple of the Kubernetes `V1Job` object and a dictionary of pod logs stored
        by pod name.

    ```python

    from prefect import flow
    from prefect_kubernetes.credentials import KubernetesCredentials
    from prefect_kubernetes.jobs import run_namespaced_job
    from prefect_kubernetes.utilites import convert_manifest_to_model

    @flow
    def kubernetes_orchestrator():
        v1_job, pod_logs = run_namespaced_job(
            kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
            job_to_run=convert_manifest_to_model("job.yaml", "V1Job"),
        )
    ```
    """
    logger = get_run_logger()

    if log_level is not None and getattr(logger, log_level.lower(), None) is None:
        raise ValueError(
            f"Invalid log level {log_level!r}. Must be one of "
            f"{['INFO', 'DEBUG', 'ERROR', 'WARN', 'CRITICAL', None]}."
        )

    if isinstance(job_to_run, (Dict, Path, str)):
        job_to_run = convert_manifest_to_model(
            manifest=job_to_run, v1_model_name="V1Job"
        )

    job_name = job_to_run.metadata.name

    await create_namespaced_job.fn(
        kubernetes_credentials=kubernetes_credentials,
        new_job=job_to_run,
        namespace=namespace,
    )

    logger.info(f"Job {job_name} created.")

    pod_log_streams = {}

    with kubernetes_credentials.get_client("batch") as batch_v1_client:

        completed = False

        while not completed:
            v1_job = await run_sync_in_worker_thread(
                batch_v1_client.read_namespaced_job_status,
                name=job_name,
                namespace=namespace,
            )
            if log_level is not None:
                log_func = getattr(logger, log_level.lower())

                pod_selector = (
                    f"controller-uid={v1_job.metadata.labels['controller-uid']}"
                )

                v1_pod_list = await list_namespaced_pod.fn(
                    kubernetes_credentials=kubernetes_credentials,
                    namespace=namespace,
                    label_selector=pod_selector,
                )

                for pod in v1_pod_list.items:
                    pod_name = pod.metadata.name

                    if pod.status.phase == "Pending" or pod_name in pod_log_streams:
                        continue

                    logger.info(f"Capturing logs for pod {pod_name}.")

                    pod_log_streams[pod_name] = await read_namespaced_pod_log.fn(
                        kubernetes_credentials=kubernetes_credentials,
                        name=pod_name,
                        namespace=namespace,
                        print_func=log_func,
                    )

            if v1_job.status.active:
                await sleep(job_status_poll_interval)
            elif v1_job.status.failed:
                raise KubernetesJobFailedError(
                    f"Job {job_name} failed, check the Kubernetes pod logs "
                    "for more information."
                )
            elif v1_job.status.succeeded:
                completed = True
                logger.info(f"Job {job_name} has completed.")

        if delete_job_after_completion:
            await delete_namespaced_job.fn(
                kubernetes_credentials=kubernetes_credentials,
                job_name=job_name,
                namespace=namespace,
            )
            logger.info(f"Job {job_name} deleted.")

        return v1_job, pod_log_streams
