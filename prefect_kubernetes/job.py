from typing import Dict, Optional

from concurrent.futures import ThreadPoolExecutor
from kubernetes import client
from prefect import task
from prefect_kubernetes.credentials import KubernetesApiKey
from prefect_kubernetes.utilities import get_kubernetes_client, KubernetesClient

@task
def create_namespaced_job(
    body: Dict = None,
    namespace: Optional[str] = "default",
    kubernetes_api_key: Optional[KubernetesApiKey] = None,
    api_client: Optional[KubernetesClient] = None,
    kube_kwargs: Optional[Dict] = {}
):
    """Task for creating a namespaced Kubernetes job.

    Args:
        body (dict): A dictionary representation of a Kubernetes V1Job
            specification. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to create this job in,
            defaults to the `default` namespace.
        kubernetes_api_key (KubernetesApiKey, optional): KubernetesApiKey block holding a Kubernetes API Key. Defaults to None.
        api_client (KubernetesClient, optional): An initialized Kubernetes API client to use to create a job. Defaults to None.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
                Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to None.

    Raises:
        ValueError: if `body` is `None`
    """
    if not body:
        raise ValueError("A dictionary representing a V1Job must be provided.")

    api_client = kubernetes_api_key.get_client(resource="job") if not api_client else api_client
    
    body = {**body, **(body or {})}
    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    api_client.create_namespaced_job(namespace=namespace, body=body, **kube_kwargs)
    
@task
def delete_namespaced_job(
    job_name: str,
    namespace: Optional[str] = "default",
    kubernetes_api_key: Optional[KubernetesApiKey] = None,
    api_client: Optional[KubernetesClient] = None,
    kube_kwargs: Optional[Dict] = {}
):
    """Task for deleting a namespaced Kubernetes job.

    Args:
        job_name (str): The name of a job to delete. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to delete this job in,
            defaults to the `default` namespace.
        kubernetes_api_key (KubernetesApiKey, optional): KubernetesApiKey block 
            holding a Kubernetes API Key. Defaults to None.
        api_client (KubernetesClient, optional): An initialized Kubernetes API client to use to delete a job. Defaults to None.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.

    Raises:
        ValueError: if `job_name` is `None`
    """
    if not job_name:
        raise ValueError("The name of a Kubernetes job must be provided.")

    api_client = kubernetes_api_key.get_client(resource="job") if not api_client else api_client

    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}
    delete_option_kwargs = delete_option_kwargs or {}

    api_client.delete_namespaced_job(
        name=job_name,
        namespace=namespace,
        body=client.V1DeleteOptions(**delete_option_kwargs),
        **kube_kwargs,
    )

@task
def list_namespaced_job(
    namespace: Optional[str] = "default",
    kubernetes_api_key: Optional[KubernetesApiKey] = None,
    api_client: Optional[KubernetesClient] = None,
    kube_kwargs: Optional[Dict] = {}
):
    """Task for listing namespaced Kubernetes jobs.

    Args:
        namespace (str, optional): The Kubernetes namespace to list jobs from,
            defaults to the `default` namespace.
        kubernetes_api_key (KubernetesApiKey, optional): KubernetesApiKey block 
            holding a Kubernetes API Key. Defaults to None.
        api_client (KubernetesClient, optional): An initialized Kubernetes API client to use to delete a job. Defaults to None.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.
    """
    api_client = kubernetes_api_key.get_client(resource="job") if not api_client else api_client

    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}
    
    api_client.list_namespaced_job(
        namespace=namespace,
        **kube_kwargs,
    )

@task
def patch_namespaced_job(
    job_name: str = None,
    namespace: Optional[str] = "default",
    kubernetes_api_key: Optional[KubernetesApiKey] = None,
    api_client: Optional[KubernetesClient] = None,
    kube_kwargs: Optional[Dict] = {}
):
    """Task for deleting a namespaced Kubernetes job.

    Args:
        job_name (str): The name of a job to patch. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to patch this job in,
            defaults to the `default` namespace.
        kubernetes_api_key (KubernetesApiKey, optional): KubernetesApiKey block 
            holding a Kubernetes API Key. Defaults to None.
        api_client (KubernetesClient, optional): An initialized Kubernetes API client to use to patch a job. Defaults to None.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.

    Raises:
        ValueError: if `job_name` is `None`
    """
    if not body:
        raise ValueError(
            "A dictionary representing a V1Job patch must be provided."
        )

    if not job_name:
        raise ValueError("The name of a Kubernetes job must be provided.")

    api_client = kubernetes_api_key.get_client(resource="job") if not api_client else api_client

    body = {**body, **(body or {})}
    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    api_client.patch_namespaced_job(
        name=job_name, namespace=namespace, body=body, **kube_kwargs
    )

@task
def read_namespaced_job(
    job_name: str = None,
    namespace: Optional[str] = "default",
    kubernetes_api_key: Optional[KubernetesApiKey] = None,
    api_client: Optional[KubernetesClient] = None,
    kube_kwargs: Optional[Dict] = {},
):
    """Task for reading a namespaced kubernetes job.

    Args:
        job_name (str): The name of a job to read. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to read this job in,
            defaults to the `default` namespace.
        kubernetes_api_key (KubernetesApiKey, optional): KubernetesApiKey block 
            holding a Kubernetes API Key. Defaults to None.
        api_client (KubernetesClient, optional): An initialized Kubernetes API client to use to read a job. Defaults to None.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.

    Raises:
        ValueError: if `job_name` is `None`
    """
    if not job_name:
        raise ValueError("The name of a Kubernetes job must be provided.")

    api_client = kubernetes_api_key.get_client(resource="job") if not api_client else api_client

    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    return api_client.read_namespaced_job(
        name=job_name, namespace=namespace, **kube_kwargs
    )

@task
def replace_namespaced_job(
    job_name: str = None,
    namespace: Optional[str] = "default",
    kubernetes_api_key: Optional[KubernetesApiKey] = None,
    api_client: Optional[KubernetesClient] = None,
    kube_kwargs: Optional[Dict] = {},
):
    """Task for replacing a namespaced kubernetes job.

    Args:
        job_name (str): The name of a job to replace. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to replace this job in,
            defaults to the `default` namespace.
        kubernetes_api_key (KubernetesApiKey, optional): KubernetesApiKey block 
            holding a Kubernetes API Key. Defaults to None.
        api_client (KubernetesClient, optional): An initialized Kubernetes API client to use to replace a job. Defaults to None.
        kube_kwargs (dict, optional): Optional extra keyword arguments to pass to the
            Kubernetes API (e.g. `{"pretty": "...", "dry_run": "..."}`). Defaults to {}.

    Raises:
        ValueError: if `job_name` is `None`
    """
    if not job_name:
        raise ValueError("The name of a Kubernetes job must be provided.")

    api_client = kubernetes_api_key.get_client(resource="job") if not api_client else api_client

    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    return api_client.replace_namespaced_job(
        name=job_name, namespace=namespace, **kube_kwargs
    )

@task
def run_namespaced_job(
    job_name: str = None,
    namespace: Optional[str] = "default",
    kubernetes_api_key: Optional[KubernetesApiKey] = None,
    kube_kwargs: Optional[Dict] = {},
    job_status_poll_interval: Optional[int] = 5,
    log_level: Optional[str] = None,
    delete_job_after_completion: Optional[bool] = True
):
    """Task for running a namespaced kubernetes job.

    Args:
        job_name (str): The name of a kubernetes job to run. Defaults to None.
        namespace (str, optional): The Kubernetes namespace to run this job in,
            defaults to the `default` namespace.
        kubernetes_api_key (KubernetesApiKey, optional): KubernetesApiKey block 
            holding a Kubernetes API Key. Defaults to None.
        api_client (KubernetesClient, optional): An initialized Kubernetes API client to use to run a job. Defaults to None.
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
        ValueError: if `body` is `None`
        ValueError: if `body["metadata"]["name"] is `None`
    """
    if not body:
        raise ValueError("A dictionary representing a V1Job must be provided.")
    
    # if log_level is not None and getattr(logger, log_level, None) is None:
    #     raise ValueError("A valid log_level must be provided.")

    body = {**body, **(body or {})}
    kube_kwargs = {**kube_kwargs, **(kube_kwargs or {})}

    job_name = body.get("metadata", {}).get("name")
    if not job_name:
        raise ValueError(
            "The job name must be defined in the body under the metadata key."
        )

    api_client_job = kubernetes_api_key.get_client(resource="job")

    api_client_pod = kubernetes_api_key.get_client(resource="pod")

    api_client_job.create_namespaced_job(
        namespace=namespace, body=body, **kube_kwargs
    )
    print(f"Job {job_name} has been created.")

    pod_log_streams = {}

    # Context is thread-local and isn't automatically copied
    # to the threads spawned by ThreadPoolExecutor.
    # Add an initializer which updates the thread's Context with
    # values from the current Context.
    # context_copy = prefect.context.copy()

    # def initialize_thread(context):
    #     prefect.context.update(context)

    # with ThreadPoolExecutor(
    #     initializer=initialize_thread, initargs=(context_copy,)
    # ) as pool:
    #     completed = False
    #     while not completed:
    #         job = api_client_job.read_namespaced_job_status(
    #             name=job_name, namespace=namespace
    #         )

    #         if log_level is not None:
    #             func_log = getattr(self.logger, log_level)

    #             pod_selector = (
    #                 f"controller-uid={job.metadata.labels['controller-uid']}"
    #             )
    #             pods_list = api_client_pod.list_namespaced_pod(
    #                 namespace=namespace, label_selector=pod_selector
    #             )

    #             for pod in pods_list.items:
    #                 pod_name = pod.metadata.name

    #                 # Can't start logs when phase is pending
    #                 if pod.status.phase == "Pending":
    #                     continue
    #                 if pod_name in pod_log_streams:
    #                     continue

    #                 read_pod_logs = ReadNamespacedPodLogs(
    #                     pod_name=pod_name,
    #                     namespace=namespace,
    #                     kubernetes_api_key=kubernetes_api_key,
    #                     on_log_entry=lambda log: func_log(f"{pod_name}: {log}"),
    #                 )

    #                 self.logger.info(f"Started following logs for {pod_name}")
    #                 pod_log_streams[pod_name] = pool.submit(read_pod_logs.run)

    #         if job.status.active:
    #             time.sleep(job_status_poll_interval)
    #         elif job.status.failed:
    #             raise signals.FAIL(
    #                 f"Job {job_name} failed, check Kubernetes pod logs for more information."
    #             )
    #         elif job.status.succeeded:
    #             self.logger.info(f"Job {job_name} has been completed.")
    #             break

    #     if delete_job_after_completion:
    #         api_client_job.delete_namespaced_job(
    #             name=job_name,
    #             namespace=namespace,
    #             body=client.V1DeleteOptions(propagation_policy="Foreground"),
    #         )
    #         self.logger.info(f"Job {job_name} has been deleted.")