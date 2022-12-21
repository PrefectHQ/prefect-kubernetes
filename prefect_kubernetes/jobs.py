"""Module to define tasks for interacting with Kubernetes jobs."""

import logging
from asyncio import sleep
from pathlib import Path
from typing import Any, Dict, Optional, Type, Union

import yaml
from kubernetes.client.models import V1DeleteOptions, V1Job, V1JobList, V1Status
from prefect import task
from prefect.blocks.abstract import JobBlock, JobRun
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from pydantic import Field, validator
from typing_extensions import Self

from prefect_kubernetes.credentials import KubernetesCredentials
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


class KubernetesJobRun(JobRun[Dict[str, Any]]):
    """A run of a Kubernetes job.

    Attributes:
        _kubernetes_job: The Kubernetes job this run is for.
    """

    def __init__(self, kubernetes_job: "KubernetesJob"):
        self._kubernetes_job = kubernetes_job
        self.pod_logs = {}

    async def wait_for_completion(self):
        """Waits for the job to complete.

        Raises:
            RuntimeError: If the Kubernetes job fails.
        """
        with self._kubernetes_job.credentials.get_client(
            "batch"
        ) as batch_v1_client, self._kubernetes_job.credentials.get_client(
            "core"
        ) as core_v1_client:
            completed = False

            while not completed:
                latest_v1_job = await run_sync_in_worker_thread(
                    batch_v1_client.read_namespaced_job_status,
                    name=self._kubernetes_job.v1_job.metadata.name,
                    namespace=self._kubernetes_job.namespace,
                    **self._kubernetes_job.api_kwargs,
                )

                v1_pod_list = await run_sync_in_worker_thread(
                    core_v1_client.list_namespaced_pod,
                    namespace=self._kubernetes_job.namespace,
                    label_selector=(
                        f"controller-uid="
                        f"{latest_v1_job.metadata.labels['controller-uid']}"
                    ),
                    **self._kubernetes_job.api_kwargs,
                )
                for pod in v1_pod_list.items:
                    pod_name = pod.metadata.name

                    if pod.status.phase == "Pending" or pod_name in self.pod_logs:
                        continue

                    self.logger.info(f"Capturing logs for pod {pod_name}.")

                    self.pod_logs[pod_name] = await run_sync_in_worker_thread(
                        core_v1_client.read_namespaced_pod_log,
                        namespace=self._kubernetes_job.namespace,
                        name=pod_name,
                        container=latest_v1_job.spec.template.spec.containers[0].name,
                        **self._kubernetes_job.api_kwargs,
                    )

                if latest_v1_job.status.active:
                    await sleep(self._kubernetes_job.interval_seconds)
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

    async def fetch_result(self) -> Dict[str, Any]:
        """Fetch the results of the job.

        Returns:
            The logs from each of the pods in the job.
        """
        if self._kubernetes_job.delete_after_completion:
            with self._kubernetes_job.credentials.get_client(
                "batch"
            ) as batch_v1_client:
                deleted_v1_job = await run_sync_in_worker_thread(
                    batch_v1_client.delete_namespaced_job,
                    namespace=self._kubernetes_job.namespace,
                    name=self._kubernetes_job.v1_job.metadata.name,
                    **self._kubernetes_job.api_kwargs,
                )
                self.logger.info(
                    f"Job {self._kubernetes_job.v1_job.metadata.name} deleted "
                    f"with {deleted_v1_job.status!r}."
                )

        return self.pod_logs


class KubernetesJob(JobBlock):
    """A block representing a Kubernetes job configuration."""

    api_kwargs: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="The kwargs to pass to all Kubernetes API calls.",
        example={"pretty": "true"},
    )
    credentials: KubernetesCredentials = Field(
        default=..., description="The credentials to configure a client from."
    )
    delete_after_completion: bool = Field(
        default=True,
        description="Whether to delete the job after it has completed.",
    )
    interval_seconds: Optional[int] = Field(
        default=5,
        description="The number of seconds to wait between job status checks.",
    )
    log_level: Optional[str] = Field(
        default="info",
        description="The log level to use when capturing logs from the job's pods.",
        example="INFO",
    )
    namespace: str = Field(
        default="default",
        description="The namespace to create and run the job in.",
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description="The number of seconds to wait for the job run before timing out.",
    )
    v1_job: Dict = Field(
        default=...,
        description=(
            "The Kubernetes job manifest to run. This dictionary can be produced "
            "using `yaml.safe_load`."
        ),
    )

    _block_type_name = "Kubernetes Job"

    class Config:
        """Add support for arbitrary types to support `V1Job` serialization."""

        arbitrary_types_allowed = True
        json_encoders = {V1Job: lambda job: job.to_dict()}

    def dict(self, *args, **kwargs) -> Dict:
        """
        Convert to a dictionary to support serialization of the `V1Job` type.
        """
        d = super().dict(*args, **kwargs)
        d["v1_job"] = d["v1_job"].to_dict()
        return d

    @validator("log_level")
    def check_valid_log_level(cls, value):
        if isinstance(value, str):
            value = value.upper()
        logging._checkLevel(value)
        return value

    def block_initialization(self):
        self.v1_job = convert_manifest_to_model(self.v1_job, "V1Job")
        super().block_initialization()

    async def trigger(self):
        """Create a Kubernetes job and return a `KubernetesJobRun` object"""
        with self.credentials.get_client("batch") as batch_v1_client:
            await run_sync_in_worker_thread(
                batch_v1_client.create_namespaced_job,
                body=self.v1_job,
                namespace=self.namespace,
                **self.api_kwargs,
            )

        return KubernetesJobRun(
            kubernetes_job=self,
        )

    @classmethod
    def from_yaml_file(
        cls: Type[Self], manifest_path: Union[Path, str], **kwargs
    ) -> Self:
        """Create a `KubernetesJob` from a YAML file.

        Args:
            manifest_path: The YAML file to create the `KubernetesJob` from.

        Returns:
            A KubernetesJob object.
        """
        with open(manifest_path, "r") as yaml_stream:
            yaml_dict = yaml.safe_load(yaml_stream)

        return cls(v1_job=yaml_dict, **kwargs)
