"""A module to define blocks and flows for running a Kubernetes job."""

from prefect import flow

from prefect_kubernetes.jobs import KubernetesJob, KubernetesJobRun


@flow
async def run_namespaced_job(
    kubernetes_job: KubernetesJob,
) -> KubernetesJobRun:
    """Flow for running a namespaced Kubernetes job.

    Args:
        kubernetes_job: The job to run.

    Returns:
        The Kubernetes `V1Job` client model and logs from each pod in the job.

    Raises:
        RuntimeError: If the job fails.

    Example:

        ```python
        from prefect_kubernetes import KubernetesJob, run_namespaced_job

        run_namespaced_job(
            kubernetes_job=KubernetesJob(v1_job="job.yaml")
        )
        ```
    """
    kubernetes_job_run = await kubernetes_job.trigger()

    await kubernetes_job_run.wait_for_completion()

    return await kubernetes_job_run.fetch_result()
