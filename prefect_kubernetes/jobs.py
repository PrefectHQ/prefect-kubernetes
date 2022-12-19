"""Module to define tasks for interacting with Kubernetes jobs."""

import logging
from asyncio import sleep
from pathlib import Path
from typing import Any, Dict, Optional, Type, Union

from kubernetes.client.models import V1DeleteOptions, V1Job, V1JobList, V1Status
from prefect import task
from prefect.blocks.abstract import JobBlock, JobRun
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from pydantic import Field, validator
from typing_extensions import Self

from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.pods import list_namespaced_pod, read_namespaced_pod_log
from prefect_kubernetes.utilities import convert_manifest_to_model

KubernetesManifest = Union[Dict, Path, str]


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


class KubernetesJobRun(JobRun):
    """A run of a Kubernetes job.

    Attributes:
        api_kwargs: The kwargs to pass to the Kubernetes API.
        credentials: The credentials to configure a client from.
        delete_after_completion: Whether to delete the job after it completes.
        interval_seconds: The number of seconds to wait between polling for job status.
        log_level: The log level to use for the job.
        namespace: The namespace to create the job in.
        pod_logs: The logs from each of the pods in the job.
        timeout_seconds: The number of seconds to wait for the job to complete.
        v1_job: A populated Kubernetes client model representing the job to run.
    """

    def __init__(
        self,
        api_kwargs: Dict[str, Any] = None,
        credentials: KubernetesCredentials = None,
        delete_after_completion: bool = True,
        interval_seconds: int = 5,
        log_level: str = None,
        timeout_seconds: int = None,
        v1_job: V1Job = None,
    ):

        self.api_kwargs = api_kwargs
        self.delete_after_completion = delete_after_completion
        self.interval_seconds = interval_seconds
        self.kubernetes_credentials = credentials
        self.log_level = log_level
        self.pod_logs = {}
        self.timeout_seconds = timeout_seconds
        self.v1_job = v1_job

    async def wait_for_completion(self):
        """Waits for the job to complete.

        Raises:
            RuntimeError: When the Kubernetes job fails.
        """
        with self.kubernetes_credentials.get_client("batch") as batch_v1_client:
            completed = False

            while not completed:
                latest_v1_job = await run_sync_in_worker_thread(
                    batch_v1_client.read_namespaced_job_status,
                    name=self.v1_job.metadata.name,
                    **self.api_kwargs,
                )

                if self.log_level is not None:
                    log_func = getattr(self.logger, self.log_level.lower())

                    v1_pod_list = await list_namespaced_pod(
                        kubernetes_credentials=self.kubernetes_credentials,
                        label_selector=(
                            f"controller-uid="
                            f"{latest_v1_job.metadata.labels['controller-uid']}"
                        ),
                        **self.api_kwargs,
                    )
                    for pod in v1_pod_list.items:
                        pod_name = pod.metadata.name

                        if pod.status.phase == "Pending" or pod_name in self.pod_logs:
                            continue

                        self.logger.info(f"Capturing logs for pod {pod_name}.")

                        self.pod_logs[pod_name] = await read_namespaced_pod_log(
                            kubernetes_credentials=self.kubernetes_credentials,
                            pod_name=pod_name,
                            container=latest_v1_job.spec.template.spec.containers[
                                0
                            ].name,
                            print_func=log_func,
                            **self.api_kwargs,
                        )

                if latest_v1_job.status.active:
                    await sleep(self.interval_seconds)
                elif latest_v1_job.status.failed:
                    raise RuntimeError(
                        f"Job {latest_v1_job.metadata.name} failed, check the "
                        "Kubernetes pod logs for more information."
                    )
                elif latest_v1_job.status.succeeded:
                    completed = True
                    self.logger.info(
                        f"Job {latest_v1_job.metadata.name} has completed."
                    )

    async def fetch_result(self: Type[Self]) -> Self:
        """Fetch the results of the job.

        Args:
            kubernetes_credentials: The credentials to configure a client from.
            v1_job: The job to fetch the results for.

        Returns:
            The job and the logs from each of the pods in the job.
        """
        if self.delete_after_completion:
            deleted_v1_job = await delete_namespaced_job(
                kubernetes_credentials=self.kubernetes_credentials,
                job_name=self.v1_job.metadata.name,
                **self.api_kwargs,
            )
            self.logger.info(
                f"Job {self.v1_job.metadata.name} deleted "
                f"with {deleted_v1_job.status!r}."
            )

        return self


class KubernetesJob(JobBlock):
    """A block representing a Kubernetes job configuration."""

    api_kwargs: Dict[str, Any] = Field(default_factory=dict)
    credentials: KubernetesCredentials = Field(default=None)
    delete_after_completion: bool = Field(default=True)
    interval_seconds: int = Field(default=5)
    log_level: str = Field(default=None)
    namespace: str = Field(default="default")
    timeout_seconds: int = Field(default=None)
    v1_job: KubernetesManifest = Field(...)

    @validator("v1_job")
    def validate_v1_job(cls, v):
        return convert_manifest_to_model(v, "V1Job")

    @validator("log_level")
    def check_valid_log_level(cls, value):
        if isinstance(value, str):
            value = value.upper()
        logging._checkLevel(value)
        return value

    async def trigger(self):
        """Triggers the job."""
        v1_job = await create_namespaced_job(
            kubernetes_credentials=self.credentials,
            new_job=self.v1_job,
            namespace=self.namespace,
            **self.api_kwargs,
        )

        return KubernetesJobRun(
            api_kwargs=self.api_kwargs or {"namespace": self.namespace},
            credentials=self.credentials,
            delete_after_completion=self.delete_after_completion,
            interval_seconds=self.interval_seconds,
            log_level=self.log_level,
            timeout_seconds=self.timeout_seconds,
            v1_job=v1_job,
        )
