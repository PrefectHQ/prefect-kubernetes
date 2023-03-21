"""
Module containing the Kubernetes worker used for executing flow runs as Kubernetes jobs.
"""
import enum
import math
import os
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional

import anyio.abc
from prefect.blocks.kubernetes import KubernetesClusterConfig
from prefect.docker import get_prefect_image_name
from prefect.experimental.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from prefect.server.schemas.core import Flow
from prefect.server.schemas.responses import DeploymentResponse
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from prefect.utilities.importtools import lazy_import
from pydantic import Field
from typing_extensions import Literal

from prefect_kubernetes.utilities import (
    _slugify_label_key,
    _slugify_label_value,
    _slugify_name,
)

if TYPE_CHECKING:
    import kubernetes
    import kubernetes.client
    import kubernetes.client.exceptions
    import kubernetes.config
    from kubernetes.client import ApiClient, BatchV1Api, CoreV1Api, V1Job, V1Pod
    from prefect.client.schemas import FlowRun

else:
    kubernetes = lazy_import("kubernetes")


def get_default_job_manifest_template() -> Dict[str, Any]:
    """Returns the default job manifest template used by the Kubernetes worker."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "labels": "{{ labels }}",
            "namespace": "{{ namespace }}",
            "generateName": "{{ name }}-",
        },
        "spec": {
            "template": {
                "spec": {
                    "parallelism": 1,
                    "completions": 1,
                    "restartPolicy": "Never",
                    "serviceAccountName": "{{ service_account_name }}",
                    "ttlSecondsAfterFinished": "{{ finished_job_ttl }}",
                    "containers": [
                        {
                            "name": "prefect-job",
                            "env": "{{ env }}",
                            "image": "{{ image }}",
                            "imagePullPolicy": "{{ image_pull_policy }}",
                            "args": "{{ command }}",
                        }
                    ],
                }
            }
        },
    }


class KubernetesImagePullPolicy(enum.Enum):
    """Enum representing the image pull policy options for a Kubernetes job."""

    IF_NOT_PRESENT = "IfNotPresent"
    ALWAYS = "Always"
    NEVER = "Never"


class KubernetesWorkerJobConfiguration(BaseJobConfiguration):
    """
    Configuration class used by the Kubernetes worker.

    An instance of this class is passed to the Kubernetes worker's `run` method
    for each flow run. It contains all of the information necessary to execute
    the flow run as a Kubernetes job.
    """

    namespace: Optional[str] = Field(default="default")
    job_manifest: Dict[str, Any] = Field(template=get_default_job_manifest_template())
    cluster_config: Optional[KubernetesClusterConfig] = Field(default=None)
    job_watch_timeout_seconds: Optional[int] = Field(default=None)
    pod_watch_timeout_seconds: int
    stream_output: bool

    # internal-use only
    _api_dns_name: Optional[str] = None  # Replaces 'localhost' in API URL

    def prepare_for_flow_run(
        self,
        flow_run: "FlowRun",
        deployment: Optional["DeploymentResponse"] = None,
        flow: Optional["Flow"] = None,
    ):
        """
        Prepares the job configuration for a flow run.

        Ensures that necessary values are present in the job manifest and that the
        job manifest is valid.
        """
        super().prepare_for_flow_run(flow_run, deployment, flow)
        # Update configuration env and job manifest env
        self._update_prefect_api_url_if_local_server()
        self.job_manifest["spec"]["template"]["spec"]["containers"][0]["env"] = [
            {"name": k, "value": v} for k, v in self.env.items()
        ]
        self._ensure_metadata_is_present()
        # Update labels in job manifest
        self._slugify_labels()
        # Add defaults to job manifest if necessary
        self._ensure_image_is_present()
        self._ensure_command_is_present()
        self._ensure_namespace_is_present()
        self._ensure_generate_name_is_present()

    def _update_prefect_api_url_if_local_server(self):
        """If the API URL has been set by the base environment rather than the by the
        user, update the value to ensure connectivity when using a bridge network by
        updating local connections to use the internal host
        """
        if self.env.get("PREFECT_API_URL") and self._api_dns_name:
            self.env["PREFECT_API_URL"] = (
                self.env["PREFECT_API_URL"]
                .replace("localhost", self._api_dns_name)
                .replace("127.0.0.1", self._api_dns_name)
            )

    def _slugify_labels(self):
        """Slugifies the labels in the job manifest."""
        try:
            self.job_manifest["metadata"]["labels"] = {
                _slugify_label_key(k): _slugify_label_value(v)
                for k, v in self.labels.items()
            }
        except KeyError:
            raise ValueError("Unable to update labels due to invalid job manifest.")

    def _ensure_image_is_present(self):
        """Ensures that the image is present in the job manifest. Populates the image
        with the default Prefect image if it is not present."""
        try:
            if (
                "image"
                not in self.job_manifest["spec"]["template"]["spec"]["containers"][0]
            ):
                self.job_manifest["spec"]["template"]["spec"]["containers"][0][
                    "image"
                ] = get_prefect_image_name()
        except KeyError:
            raise ValueError(
                "Unable to verify image due to invalid job manifest template."
            )

    def _ensure_command_is_present(self):
        """
        Ensures that the command is present in the job manifest. Populates the command
        with the `prefect -m prefect.engine` if a command is not present.
        """
        try:
            command = self.job_manifest["spec"]["template"]["spec"]["containers"][
                0
            ].get("args")
            if command is None:
                self.job_manifest["spec"]["template"]["spec"]["containers"][0][
                    "args"
                ] = [
                    "python",
                    "-m",
                    "prefect.engine",
                ]
            elif isinstance(command, str):
                self.job_manifest["spec"]["template"]["spec"]["containers"][0][
                    "args"
                ] = command.split()
            elif not isinstance(command, list):
                raise ValueError(
                    "Invalid job manifest template: 'command' must be a string or list."
                )
        except KeyError:
            raise ValueError(
                "Unable to verify command due to invalid job manifest template."
            )

    def _ensure_metadata_is_present(self):
        """Ensures that the metadata is present in the job manifest."""
        if "metadata" not in self.job_manifest:
            self.job_manifest["metadata"] = {}

    def _ensure_namespace_is_present(self):
        """Ensures that the namespace is present in the job manifest."""
        try:
            if "namespace" not in self.job_manifest["metadata"]:
                self.job_manifest["metadata"]["namespace"] = self.namespace
        except KeyError:
            raise ValueError(
                "Unable to verify namespace due to invalid job manifest template."
            )

    def _ensure_generate_name_is_present(self):
        """Ensures that the generateName is present in the job manifest."""
        try:
            generate_name = None
            if self.name:
                generate_name = _slugify_name(self.name)
            # _slugify_name will return None if the slugified name in an exception
            if not generate_name:
                generate_name = "prefect-job"
            self.job_manifest["metadata"]["generateName"] = f"{generate_name}-"
        except KeyError:
            raise ValueError(
                "Unable to verify generateName due to invalid job manifest template."
            )


class KubernetesWorkerVariables(BaseVariables):
    """
    Default variables for the Kubernetes worker.

    The schema for this class is used to populate the `variables` section of the default
    base job template.
    """

    namespace: str = Field(
        default="default", description="The Kubernetes namespace to create jobs within."
    )
    image: Optional[str] = Field(
        default=None,
        description="The image reference of a container image to use for created jobs. "
        "If not set, the latest Prefect image will be used.",
        example="docker.io/prefecthq/prefect:2-latest",
    )
    service_account_name: Optional[str] = Field(
        default=None,
        description="The Kubernetes service account to use for job creation.",
    )
    image_pull_policy: Literal["IfNotPresent", "Always", "Never"] = Field(
        default=KubernetesImagePullPolicy.IF_NOT_PRESENT,
        description="The Kubernetes image pull policy to use for job containers.",
    )
    finished_job_ttl: Optional[int] = Field(
        default=None,
        title="Finished Job TTL",
        description="The number of seconds to retain jobs after completion. If set, "
        "finished jobs will be cleaned up by Kubernetes after the given delay. If not "
        "set, jobs will be retained indefinitely.",
    )
    job_watch_timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Number of seconds to wait for each event emitted by a job before "
            "timing out. If not set, the worker will wait for each event indefinitely."
        ),
    )
    pod_watch_timeout_seconds: int = Field(
        default=60,
        description="Number of seconds to watch for pod creation before timing out.",
    )
    stream_output: bool = Field(
        default=True,
        description=(
            "If set, output will be streamed from the job to local standard output."
        ),
    )
    cluster_config: Optional[KubernetesClusterConfig] = Field(
        default=None,
        description="The Kubernetes cluster config to use for job creation.",
    )


class KubernetesWorkerResult(BaseWorkerResult):
    """Contains information about the final state of a completed process"""


class KubernetesWorker(BaseWorker):
    """Prefect worker that executes flow runs within Kubernetes Jobs."""

    type = "kubernetes"
    job_configuration = KubernetesWorkerJobConfiguration
    job_configuration_variables = KubernetesWorkerVariables

    async def run(
        self,
        flow_run: "FlowRun",
        configuration: KubernetesWorkerJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> KubernetesWorkerResult:
        """
        Executes a flow run within a Kubernetes Job and waits for the flow run
        to complete.

        Args:
            flow_run: The flow run to execute
            configuration: The configuration to use when executing the flow run.
            task_status: The task status object for the current flow run. If provided,
                the task will be marked as started.

        Returns:
            KubernetesWorkerResult: A result object containing information about the
                final state of the flow run
        """
        with self._get_configured_kubernetes_client(configuration) as client:
            job = await run_sync_in_worker_thread(
                self._create_job, configuration, client
            )
            pid = await run_sync_in_worker_thread(
                self._get_infrastructure_pid, job, client
            )
            # Indicate that the job has started
            if task_status is not None:
                task_status.started(pid)

            # Monitor the job until completion
            status_code = await run_sync_in_worker_thread(
                self._watch_job, job.metadata.name, configuration, client
            )
            return KubernetesWorkerResult(identifier=pid, status_code=status_code)

    def _get_configured_kubernetes_client(
        self, configuration: KubernetesWorkerJobConfiguration
    ) -> "ApiClient":
        """
        Returns a configured Kubernetes client.
        """

        # if a hard-coded cluster config is provided, use it
        if configuration.cluster_config:
            return kubernetes.config.new_client_from_config_dict(
                config_dict=configuration.cluster_config.config,
                context=configuration.cluster_config.context_name,
            )
        else:
            # If no hard-coded config specified, try to load Kubernetes configuration
            # within a cluster. If that doesn't work, try to load the configuration
            # from the local environment, allowing any further ConfigExceptions to
            # bubble up.
            try:
                kubernetes.config.load_incluster_config()
                config = kubernetes.client.Configuration.get_default_copy()
                return kubernetes.client.ApiClient(configuration=config)
            except kubernetes.config.ConfigException:
                return kubernetes.config.new_client_from_config()

    def _create_job(
        self, configuration: KubernetesWorkerJobConfiguration, client: "ApiClient"
    ) -> "V1Job":
        """
        Creates a Kubernetes job from a job manifest.
        """
        with self._get_batch_client(client) as batch_client:
            job = batch_client.create_namespaced_job(
                configuration.namespace, configuration.job_manifest
            )
        return job

    @contextmanager
    def _get_batch_client(
        self, client: "ApiClient"
    ) -> Generator["BatchV1Api", None, None]:
        """
        Context manager for retrieving a Kubernetes batch client.
        """
        try:
            yield kubernetes.client.BatchV1Api(api_client=client)
        finally:
            client.rest_client.pool_manager.clear()

    def _get_infrastructure_pid(self, job: "V1Job", client: "ApiClient") -> str:
        """
        Generates a Kubernetes infrastructure PID.

        The PID is in the format: "<cluster uid>:<namespace>:<job name>".
        """
        cluster_uid = self._get_cluster_uid(client)
        pid = f"{cluster_uid}:{job.metadata.namespace}:{job.metadata.name}"
        return pid

    @contextmanager
    def _get_core_client(
        self, client: "ApiClient"
    ) -> Generator["CoreV1Api", None, None]:
        """
        Context manager for retrieving a Kubernetes core client.
        """
        try:
            yield kubernetes.client.CoreV1Api(api_client=client)
        finally:
            client.rest_client.pool_manager.clear()

    def _get_cluster_uid(self, client: "ApiClient") -> str:
        """
        Gets a unique id for the current cluster being used.

        There is no real unique identifier for a cluster. However, the `kube-system`
        namespace is immutable and has a persistence UID that we use instead.

        PREFECT_KUBERNETES_CLUSTER_UID can be set in cases where the `kube-system`
        namespace cannot be read e.g. when a cluster role cannot be created. If set,
        this variable will be used and we will not attempt to read the `kube-system`
        namespace.

        See https://github.com/kubernetes/kubernetes/issues/44954
        """
        # Default to an environment variable
        env_cluster_uid = os.environ.get("PREFECT_KUBERNETES_CLUSTER_UID")
        if env_cluster_uid:
            return env_cluster_uid

        # Read the UID from the cluster namespace
        with self._get_core_client(client) as core_client:
            namespace = core_client.read_namespace("kube-system")
        cluster_uid = namespace.metadata.uid

        return cluster_uid

    def _watch_job(
        self,
        job_name: str,
        configuration: KubernetesWorkerJobConfiguration,
        client: "ApiClient",
    ) -> int:
        """
        Watch a job.

        Return the final status code of the first container.
        """
        self._logger.debug(f"Job {job_name!r}: Monitoring job...")

        job = self._get_job(job_name, configuration, client)
        if not job:
            return -1

        pod = self._get_job_pod(job_name, configuration, client)
        if not pod:
            return -1

        # Calculate the deadline before streaming output
        deadline = (
            (time.monotonic() + configuration.job_watch_timeout_seconds)
            if configuration.job_watch_timeout_seconds is not None
            else None
        )

        if configuration.stream_output:
            with self._get_core_client(client) as core_client:
                logs = core_client.read_namespaced_pod_log(
                    pod.metadata.name,
                    configuration.namespace,
                    follow=True,
                    _preload_content=False,
                    container="prefect-job",
                )
                try:
                    for log in logs.stream():
                        print(log.decode().rstrip())

                        # Check if we have passed the deadline and should stop streaming
                        # logs
                        remaining_time = (
                            deadline - time.monotonic() if deadline else None
                        )
                        if deadline and remaining_time <= 0:
                            break

                except Exception:
                    self._logger.warning(
                        (
                            "Error occurred while streaming logs - "
                            "Job will continue to run but logs will "
                            "no longer be streamed to stdout."
                        ),
                        exc_info=True,
                    )

        with self._get_batch_client(client) as batch_client:
            # Check if the job is completed before beginning a watch
            job = batch_client.read_namespaced_job(
                name=job_name, namespace=configuration.namespace
            )
            completed = job.status.completion_time is not None

            while not completed:
                remaining_time = (
                    math.ceil(deadline - time.monotonic()) if deadline else None
                )
                if deadline and remaining_time <= 0:
                    self._logger.error(
                        f"Job {job_name!r}: Job did not complete within "
                        f"timeout of {configuration.job_watch_timeout_seconds}s."
                    )
                    return -1

                watch = kubernetes.watch.Watch()
                # The kubernetes library will disable retries if the timeout kwarg is
                # present regardless of the value so we do not pass it unless given
                # https://github.com/kubernetes-client/python/blob/84f5fea2a3e4b161917aa597bf5e5a1d95e24f5a/kubernetes/base/watch/watch.py#LL160
                timeout_seconds = (
                    {"timeout_seconds": remaining_time} if deadline else {}
                )

                for event in watch.stream(
                    func=batch_client.list_namespaced_job,
                    field_selector=f"metadata.name={job_name}",
                    namespace=configuration.namespace,
                    **timeout_seconds,
                ):
                    if event["object"].status.completion_time:
                        if not event["object"].status.succeeded:
                            # Job failed, exit while loop and return pod exit code
                            self._logger.error(f"Job {job_name!r}: Job failed.")
                        completed = True
                        watch.stop()
                        break

        with self._get_core_client(client) as core_client:
            pod_status = core_client.read_namespaced_pod_status(
                namespace=configuration.namespace, name=pod.metadata.name
            )
            first_container_status = pod_status.status.container_statuses[0]

        return first_container_status.state.terminated.exit_code

    def _get_job(
        self,
        job_id: str,
        configuration: KubernetesWorkerJobConfiguration,
        client: "ApiClient",
    ) -> Optional["V1Job"]:
        """Get a Kubernetes job by id."""
        with self._get_batch_client(client) as batch_client:
            try:
                job = batch_client.read_namespaced_job(
                    name=job_id, namespace=configuration.namespace
                )
            except kubernetes.client.exceptions.ApiException:
                self._logger.error(f"Job {job_id!r} was removed.", exc_info=True)
                return None
            return job

    def _get_job_pod(
        self,
        job_name: str,
        configuration: KubernetesWorkerJobConfiguration,
        client: "ApiClient",
    ) -> Optional["V1Pod"]:
        """Get the first running pod for a job."""
        watch = kubernetes.watch.Watch()
        self._logger.debug(f"Job {job_name!r}: Starting watch for pod start...")
        last_phase = None
        with self._get_core_client(client) as core_client:
            for event in watch.stream(
                func=core_client.list_namespaced_pod,
                namespace=configuration.namespace,
                label_selector=f"job-name={job_name}",
                timeout_seconds=configuration.pod_watch_timeout_seconds,
            ):
                phase = event["object"].status.phase
                if phase != last_phase:
                    self._logger.info(f"Job {job_name!r}: Pod has status {phase!r}.")

                if phase != "Pending":
                    watch.stop()
                    return event["object"]

                last_phase = phase

        self._logger.error(f"Job {job_name!r}: Pod never started.")
