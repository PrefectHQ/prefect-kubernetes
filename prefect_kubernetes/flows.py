"""A module to define flows interacting with Kubernetes resources."""

from typing import Any, Dict

from prefect import flow, task

from prefect_kubernetes.jobs import KubernetesJob


@task
async def trigger(kubernetes_job):
    """Task for triggering a Kubernetes job."""
    return await kubernetes_job.trigger()


@task
async def wait_for_completion(kubernetes_job_run):
    """Task for waiting for a Kubernetes job to complete."""
    await kubernetes_job_run.wait_for_completion()


@task
async def fetch_result(kubernetes_job_run):
    """Task for fetching the result of a Kubernetes job."""
    return await kubernetes_job_run.fetch_result()


@flow
async def run_namespaced_job(
    kubernetes_job: KubernetesJob,
) -> Dict[str, Any]:
    """Flow for running a namespaced Kubernetes job.

    Args:
        kubernetes_job: The `KubernetesJob` block that specifies the job to run.

    Returns:
        The a dict of logs from each pod in the job, e.g. {'pod_name': 'pod_log_str'}.

    Raises:
        RuntimeError: If the created Kubernetes job attains a failed status.

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
    kubernetes_job_run = await trigger(kubernetes_job)

    await wait_for_completion(kubernetes_job_run)

    return await fetch_result(kubernetes_job_run)
