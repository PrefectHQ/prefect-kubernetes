"""A module to define flows interacting with Kubernetes resources."""

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
        The logs from each pod in the job.

    Raises:
        RuntimeError: If the job fails.

    Example:

        ```python
        from prefect_kubernetes import KubernetesJob, run_namespaced_job
        from prefect_kubernetes.credentials import KubernetesCredentials

        run_namespaced_job(
            kubernetes_job=KubernetesJob.from_yaml_file(
                credentials=KubernetesCredentials.load("k8s-creds"),
                manifest_path="path/to/job.yaml",
            )
        )
        ```
    """
    kubernetes_job_run = await kubernetes_job.trigger()

    await kubernetes_job_run.wait_for_completion()

    return await kubernetes_job_run.fetch_result()