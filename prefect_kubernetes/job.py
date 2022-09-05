import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional

import prefect.context
from kubernetes import client
from prefect import get_run_logger, task

from prefect_kubernetes import exceptions as err
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.pod import read_namespaced_pod_logs


@task
async def create_namespaced_job(
    body: Dict,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = None,
):
    """Task for creating a namespaced Kubernetes job.

    Args:
        body (dict): A dictionary representation of a Kubernetes V1Job
            specification.
        namespace (str, optional): The Kubernetes namespace to create this job in,
            defaults to the `default` namespace.
        kubernetes_credentials (KubernetesCredentials): KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to None.
    """
    api_client = kubernetes_credentials.get_batch_client()

    api_client.create_namespaced_job(namespace=namespace, body=body, **kube_kwargs)


@task
async def delete_namespaced_job(
    job_name: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = None,
    delete_option_kwargs: Optional[Dict] = None,
):
    """Task for deleting a namespaced Kubernetes job.

    Args:
        job_name (str): The name of a job to delete. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to delete this job in,
            defaults to the `default` namespace.
        kubernetes_credentials (KubernetesCredentials): KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.
        - delete_option_kwargs (dict, optional): Optional keyword arguments to pass to
            the V1DeleteOptions object (e.g. {"propagation_policy": "...",
            "grace_period_seconds": "..."}.
    """

    api_client = kubernetes_credentials.get_batch_client()

    kwargs = kube_kwargs or {}

    if delete_option_kwargs:
        kwargs.update(body=client.V1DeleteOptions(**delete_option_kwargs))

    api_client.delete_namespaced_job(name=job_name, namespace=namespace, **kwargs)


@task
async def list_namespaced_job(
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = None,
):
    """Task for listing namespaced Kubernetes jobs.

    Args:
        kubernetes_credentials (KubernetesCredentials): KubernetesCredentials block
            holding authentication needed to generate the required API client.
        namespace (str, optional): The Kubernetes namespace to list jobs from,
            defaults to the `default` namespace.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.
    """
    api_client = kubernetes_credentials.get_batch_client()

    method_kwargs = kube_kwargs or {}

    api_client.list_namespaced_job(
        namespace=namespace,
        **method_kwargs,
    )


@task
async def patch_namespaced_job(
    job_name: str,
    body: dict,
    kubernetes_credentials: KubernetesCredentials = None,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = {},
):
    """Task for deleting a namespaced Kubernetes job.

    Args:
        job_name (str): The name of a job to patch. Defaults to None.
        body (dict): A dictionary representation of a Kubernetes V1Job
            specification. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to patch this job in,
            defaults to the `default` namespace.
        kubernetes_credentials (KubernetesCredentials): KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.

    Raises:
        ValueError: if `job_name` is `None`
    """
    if not body:
        raise err.KubernetesJobDefinitionError(
            "A dictionary representing a V1Job must be provided."
        )

    if not job_name:
        raise ValueError("The name of a Kubernetes job must be provided.")

    api_client = kubernetes_credentials.get_batch_client()

    body = {**body, **(body or {})}
    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    api_client.patch_namespaced_job(
        name=job_name, namespace=namespace, body=body, **kube_kwargs
    )


@task
async def read_namespaced_job(
    job_name: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = {},
):
    """Task for reading a namespaced kubernetes job.

    Args:
        job_name (str): The name of a job to read. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to read this job in,
            defaults to the `default` namespace.
        kubernetes_credentials (KubernetesCredentials): KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.

    Raises:
        ValueError: if `job_name` is `None`
    """
    if not job_name:
        raise ValueError("The name of a Kubernetes job must be provided.")

    api_client = kubernetes_credentials.get_batch_client()

    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    return api_client.read_namespaced_job(
        name=job_name, namespace=namespace, **kube_kwargs
    )


@task
async def replace_namespaced_job(
    body: dict,
    job_name: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = {},
):
    """Task for replacing a namespaced kubernetes job.

    Args:
        body (dict, optional): A dictionary representation of a Kubernetes V1Job
            specification
        job_name (str): The name of a job to replace. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to replace this job in,
            defaults to the `default` namespace.
        kubernetes_credentials (KubernetesCredentials): KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.

    Raises:
        ValueError: if `body` is `None`
        ValueError: if `job_name` is `None`
    """
    if not body:
        raise err.KubernetesJobDefinitionError(
            "A dictionary representing a V1Job must be provided."
        )

    if not job_name:
        raise ValueError("The name of a Kubernetes job must be provided.")

    api_client = kubernetes_credentials.get_batch_client()

    body = {**body, **(body or {})}
    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    return api_client.replace_namespaced_job(
        name=job_name, body=body, namespace=namespace, **kube_kwargs
    )


@task
async def run_namespaced_job(
    body: str,
    kubernetes_credentials: KubernetesCredentials,
    namespace: Optional[str] = "default",
    kube_kwargs: Optional[Dict] = {},
    job_status_poll_interval: Optional[int] = 5,
    log_level: Optional[str] = None,
    delete_job_after_completion: Optional[bool] = True,
):
    """Task for running a namespaced kubernetes job.

    Args:
        body (str): A dictionary representation of a Kubernetes V1Job
            specification. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to run this job in,
            defaults to the `default` namespace.
        kubernetes_credentials (KubernetesCredentials): KubernetesCredentials block
            holding authentication needed to generate the required API client.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.
        job_status_poll_interval (int, optional): The interval given in seconds
            indicating how often the Kubernetes API will be requested about the status
            of the job being performed, defaults to `5` seconds.
        log_level (str, optional): Log level used when outputting logs from the job
            should be one of `debug`, `info`, `warn`, `error`, 'critical' or `None` to
            disable output completely. Defaults to `None`.
        delete_job_after_completion (bool, optional): boolean value determining whether
            resources related to a given job will be removed from the Kubernetes cluster
            after completion, defaults to the `True` value
    Raises:
        KubernetesJobDefinitionError: if `body` is `None`
        KubernetesJobDefinitionError: if `body["metadata"]["name"] is `None`
    """
    logger = get_run_logger().setLevel(level=log_level)

    if not body:
        raise err.KubernetesJobDefinitionError(
            "A dictionary representing a V1Job must be provided."
        )

    # if log_level is not None and getattr(logger, log_level, None) is None:
    #     raise ValueError("A valid log_level must be provided.")

    body = {**body, **(body or {})}
    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    job_name = body.get("metadata", {}).get("name")

    if not job_name:
        raise err.KubernetesJobDefinitionError(
            "The job name must be defined in the body under the metadata key."
        )

    api_batch_client = kubernetes_credentials.get_batch_client()
    api_core_client = kubernetes_credentials.get_core_client()

    api_batch_client.create_namespaced_job(
        namespace=namespace, body=body, **kube_kwargs
    )
    logger.info(f"Job {job_name} has been created.")

    pod_log_streams = {}

    # Context is thread-local and isn't automatically copied to the threads spawned by ThreadPoolExecutor.
    # Add an initializer which updates the thread's Context with values from the current Context.
    context_copy = prefect.context.copy()

    def initialize_thread(context):
        prefect.context.update(context)

    with ThreadPoolExecutor(
        initializer=initialize_thread, initargs=(context_copy,)
    ) as pool:
        completed = False
        while not completed:
            job = api_batch_client.read_namespaced_job_status(
                name=job_name, namespace=namespace
            )
            if log_level is not None:
                func_log = getattr(logger, log_level)

                pod_selector = f"controller-uid={job.metadata.labels['controller-uid']}"
                pods_list = api_core_client.list_namespaced_pod(
                    namespace=namespace, label_selector=pod_selector
                )

                for pod in pods_list.items:
                    pod_name = pod.metadata.name

                    # Can't start logs when phase is pending
                    if pod.status.phase == "Pending":
                        continue
                    if pod_name in pod_log_streams:
                        continue

                    read_pod_logs = read_namespaced_pod_logs(
                        pod_name=pod_name,
                        namespace=namespace,
                        kubernetes_credentials=kubernetes_credentials,
                        on_log_entry=lambda log: func_log(f"{pod_name}: {log}"),
                    )

                    logger.info(f"Started following logs for {pod_name}")
                    pod_log_streams[pod_name] = pool.submit(read_pod_logs.run)

            if job.status.active:
                time.sleep(job_status_poll_interval)
            elif job.status.failed:
                raise err.KubernetesJobFailedError(
                    f"Job {job_name} failed, check Kubernetes pod logs for more information."
                )
            elif job.status.succeeded:
                logger.info(f"Job {job_name} has been completed.")
                break

            if delete_job_after_completion:
                api_batch_client.delete_namespaced_job(
                    name=job_name,
                    namespace=namespace,
                    body=client.V1DeleteOptions(propagation_policy="Foreground"),
                )
                logger.info(f"Job {job_name} has been deleted.")
