"""A module to define blocks and flows for running a Kubernetes job."""

from asyncio import sleep
from pathlib import Path
from typing import Any, Dict, Tuple, Union

from kubernetes.client import V1Job
from prefect import flow, get_run_logger
from prefect.blocks.core import Block
from prefect.exceptions import MissingContextError
from prefect.logging import get_logger
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from pydantic import Field

from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.jobs import create_namespaced_job, delete_namespaced_job
from prefect_kubernetes.pods import list_namespaced_pod, read_namespaced_pod_log
from prefect_kubernetes.utilities import convert_manifest_to_model

KubernetesManifest = Union[Dict, Path, str]


class KubernetesRunJob(Block):
    """A block for running a Kubernetes job."""

    @property
    def logger(self):
        """Returns the logger to use for this block."""
        try:
            return get_run_logger()
        except MissingContextError:
            return get_logger()

    @property
    def v1_job(self):
        """Returns the `V1Job` object to use for this block."""
        return convert_manifest_to_model(self.job_to_run, "V1Job")

    @property
    def pod_selector(self):
        """Returns the pod selector to use for this block."""
        return f"controller-uid={self.v1_job.metadata.labels['controller-uid']}"

    kubernetes_credentials: KubernetesCredentials
    job_to_run: KubernetesManifest

    api_kwargs: Dict[str, Any] = Field(default_factory=dict)
    delete_after_completion: bool = Field(
        default=True, description="Whether to delete the job after it has completed."
    )
    job_status_poll_interval: int = Field(
        default=5, description="The interval to poll for job status."
    )
    log_level: str = Field(
        default=None, description="The log level to use when logging job logs."
    )
    pog_logs: Dict[str, Any] = Field(default_factory=dict)

    async def trigger(self):
        """Triggers the job."""

        return await create_namespaced_job(
            kubernetes_credentials=self.kubernetes_credentials,
            new_job=self.v1_job,
            **self.api_kwargs,
        )

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
                        label_selector=self.pod_selector,
                        **self.api_kwargs,
                    )
                    for pod in v1_pod_list.items:
                        pod_name = pod.metadata.name

                        if pod.status.phase == "Pending" or pod_name in self.pog_logs:
                            continue

                        self.logger.info(f"Capturing logs for pod {pod_name}.")

                        self.pod_logs[pod_name] = await read_namespaced_pod_log(
                            kubernetes_credentials=self.kubernetes_credentials,
                            name=pod_name,
                            print_func=log_func,
                            **self.api_kwargs,
                        )

                if latest_v1_job.status.active:
                    await sleep(self.job_status_poll_interval)
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

    async def fetch_results(self) -> Tuple[V1Job, Dict[str, Any]]:
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

        return self.v1_job


@flow
async def run_namespaced_job(
    kubernetes_job: KubernetesRunJob,
) -> Tuple[V1Job, Dict[str, str]]:
    """Flow for running a namespaced Kubernetes job."""
    await kubernetes_job.trigger()

    await kubernetes_job.wait_for_completion()

    return await kubernetes_job.fetch_results()
