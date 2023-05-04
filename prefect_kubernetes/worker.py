"""
<span class="badge-api beta"/>

Module containing the Kubernetes worker used for executing flow runs as Kubernetes jobs.

Note this module is in **beta**. The interfaces within may change without notice.

To start a Kubernetes worker, run the following command:

```bash
prefect worker start --pool 'my-work-pool' --type kubernetes
```

Replace `my-work-pool` with the name of the work pool you want the worker
to poll for flow runs.

!!! example "Using a custom Kubernetes job manifest template"
    The default template used for Kubernetes job manifests looks like this:
    ```yaml
    ---
    apiVersion: batch/v1
    kind: Job
    metadata:
    labels: "{{ labels }}"
    namespace: "{{ namespace }}"
    generateName: "{{ name }}-"
    spec:
    ttlSecondsAfterFinished: "{{ finished_job_ttl }}"
    template:
        spec:
        parallelism: 1
        completions: 1
        restartPolicy: Never
        serviceAccountName: "{{ service_account_name }}"
        containers:
        - name: prefect-job
            env: "{{ env }}"
            image: "{{ image }}"
            imagePullPolicy: "{{ image_pull_policy }}"
            args: "{{ command }}"
    ```

    Each values enclosed in `{{ }}` is a placeholder that will be replaced with
    a value at runtime. The values that can be used a placeholders are defined
    by the `variables` schema defined in the base job template.

    The default job manifest and available variables can be customized on a work pool
    by work pool basis. These customizations can be made via the Prefect UI when
    creating or editing a work pool.

    For example, if you wanted to allow custom memory requests for a Kubernetes work
    pool you could update the job manifest template to look like this:

    ```yaml
    ---
    apiVersion: batch/v1
    kind: Job
    metadata:
    labels: "{{ labels }}"
    namespace: "{{ namespace }}"
    generateName: "{{ name }}-"
    spec:
    ttlSecondsAfterFinished: "{{ finished_job_ttl }}"
    template:
        spec:
        parallelism: 1
        completions: 1
        restartPolicy: Never
        serviceAccountName: "{{ service_account_name }}"
        containers:
        - name: prefect-job
            env: "{{ env }}"
            image: "{{ image }}"
            imagePullPolicy: "{{ image_pull_policy }}"
            args: "{{ command }}"
            resources:
                requests:
                    memory: "{{ memory }}Mi"
                limits:
                    memory: 128Mi
    ```

    In this new template, the `memory` placeholder allows customization of the memory
    allocated to Kubernetes jobs created by workers in this work pool, but the limit
    is hard-coded and cannot be changed by deployments.

For more information about work pools and workers,
checkout out the [Prefect docs](https://docs.prefect.io/concepts/work-pools/).
"""
import enum
import math
import os
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple

import anyio.abc
from prefect.blocks.kubernetes import KubernetesClusterConfig
from prefect.docker import get_prefect_image_name
from prefect.events import RelatedResource
from prefect.events.related import object_as_related_resource, tags_as_related_resources
from prefect.exceptions import InfrastructureNotAvailable, InfrastructureNotFound
from prefect.server.schemas.core import Flow
from prefect.server.schemas.responses import DeploymentResponse
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from prefect.utilities.importtools import lazy_import
from prefect.utilities.pydantic import JsonPatch
from prefect.utilities.templating import find_placeholders
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field, validator
from typing_extensions import Literal

from prefect_kubernetes.events import KubernetesEventsReplicator
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
    import kubernetes.watch
    from kubernetes.client import ApiClient, BatchV1Api, CoreV1Api, V1Job, V1Pod
    from prefect.client.schemas import FlowRun
else:
    kubernetes = lazy_import("kubernetes")


def _get_default_job_manifest_template() -> Dict[str, Any]:
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
            "ttlSecondsAfterFinished": "{{ finished_job_ttl }}",
            "template": {
                "spec": {
                    "parallelism": 1,
                    "completions": 1,
                    "restartPolicy": "Never",
                    "serviceAccountName": "{{ service_account_name }}",
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
            },
        },
    }


def _get_base_job_manifest():
    """Returns a base job manifest to use for manifest validation."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"labels": {}},
        "spec": {
            "template": {
                "spec": {
                    "parallelism": 1,
                    "completions": 1,
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "prefect-job",
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

    Attributes:
        name: The name to give to created Kubernetes job.
        command: The command executed in created Kubernetes jobs to kick off
            flow run execution.
        env: The environment variables to set in created Kubernetes jobs.
        labels: The labels to set on created Kubernetes jobs.
        namespace: The Kubernetes namespace to create Kubernetes jobs in.
        job_manifest: The Kubernetes job manifest to use to create Kubernetes jobs.
        cluster_config: The Kubernetes cluster configuration to use for authentication
            to a Kubernetes cluster.
        job_watch_timeout_seconds: The number of seconds to wait for the job to
            complete before timing out. If `None`, the worker will wait indefinitely.
        pod_watch_timeout_seconds: The number of seconds to wait for the pod to
            complete before timing out.
        stream_output: Whether or not to stream the job's output.
    """

    namespace: str = Field(default="default")
    job_manifest: Dict[str, Any] = Field(template=_get_default_job_manifest_template())
    cluster_config: Optional[KubernetesClusterConfig] = Field(default=None)
    job_watch_timeout_seconds: Optional[int] = Field(default=None)
    pod_watch_timeout_seconds: int = Field(default=60)
    stream_output: bool = Field(default=True)

    # internal-use only
    _api_dns_name: Optional[str] = None  # Replaces 'localhost' in API URL

    @validator("job_manifest")
    def _ensure_metadata_is_present(cls, value: Dict[str, Any]):
        """Ensures that the metadata is present in the job manifest."""
        if "metadata" not in value:
            value["metadata"] = {}
        return value

    @validator("job_manifest")
    def _ensure_labels_is_present(cls, value: Dict[str, Any]):
        """Ensures that the metadata is present in the job manifest."""
        if "labels" not in value["metadata"]:
            value["metadata"]["labels"] = {}
        return value

    @validator("job_manifest")
    def _ensure_namespace_is_present(cls, value: Dict[str, Any], values):
        """Ensures that the namespace is present in the job manifest."""
        if "namespace" not in value["metadata"]:
            value["metadata"]["namespace"] = values["namespace"]
        return value

    @validator("job_manifest")
    def _ensure_job_includes_all_required_components(cls, value: Dict[str, Any]):
        """
        Ensures that the job manifest includes all required components.
        """
        patch = JsonPatch.from_diff(value, _get_base_job_manifest())
        missing_paths = sorted([op["path"] for op in patch if op["op"] == "add"])
        if missing_paths:
            raise ValueError(
                "Job is missing required attributes at the following paths: "
                f"{', '.join(missing_paths)}"
            )
        return value

    @validator("job_manifest")
    def _ensure_job_has_compatible_values(cls, value: Dict[str, Any]):
        patch = JsonPatch.from_diff(value, _get_base_job_manifest())
        incompatible = sorted(
            [
                f"{op['path']} must have value {op['value']!r}"
                for op in patch
                if op["op"] == "replace"
            ]
        )
        if incompatible:
            raise ValueError(
                "Job has incompatible values for the following attributes: "
                f"{', '.join(incompatible)}"
            )
        return value

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

        Args:
            flow_run: The flow run to prepare the job configuration for
            deployment: The deployment associated with the flow run used for
                preparation.
            flow: The flow associated with the flow run used for preparation.
        """
        super().prepare_for_flow_run(flow_run, deployment, flow)
        # Update configuration env and job manifest env
        self._update_prefect_api_url_if_local_server()
        self.job_manifest["spec"]["template"]["spec"]["containers"][0]["env"] = [
            {"name": k, "value": v} for k, v in self.env.items()
        ]
        # Update labels in job manifest
        self._slugify_labels()
        # Add defaults to job manifest if necessary
        self._populate_image_if_not_present()
        self._populate_command_if_not_present()
        self._populate_generate_name_if_not_present()

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
        all_labels = {**self.job_manifest["metadata"].get("labels", {}), **self.labels}
        self.job_manifest["metadata"]["labels"] = {
            _slugify_label_key(k): _slugify_label_value(v)
            for k, v in all_labels.items()
        }

    def _populate_image_if_not_present(self):
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

    def _populate_command_if_not_present(self):
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

    def _populate_generate_name_if_not_present(self):
        """Ensures that the generateName is present in the job manifest."""
        manifest_generate_name = self.job_manifest["metadata"].get("generateName", "")
        has_placeholder = len(find_placeholders(manifest_generate_name)) > 0
        if not manifest_generate_name or has_placeholder:
            generate_name = None
            if self.name:
                generate_name = _slugify_name(self.name)
            # _slugify_name will return None if the slugified name in an exception
            if not generate_name:
                generate_name = "prefect-job"
            self.job_manifest["metadata"]["generateName"] = f"{generate_name}-"

    def _related_resources(self) -> List[RelatedResource]:
        tags = set()
        related = []

        for kind, obj in self._related_objects.items():
            # TODO: Remove this method once we've updated the Prefect core side
            # to ignore objects that are None.
            if not obj:
                continue
            if hasattr(obj, "tags"):
                tags.update(obj.tags)
            related.append(object_as_related_resource(kind=kind, role=kind, object=obj))

        return related + tags_as_related_resources(tags)


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

            events_replicator = KubernetesEventsReplicator(
                client=client,
                job_name=job.metadata.name,
                namespace=configuration.namespace,
                worker_resource=self._event_resource(),
                related_resources=self._event_related_resources(
                    configuration=configuration
                ),
                timeout_seconds=configuration.pod_watch_timeout_seconds,
            )

            with events_replicator:
                status_code = await run_sync_in_worker_thread(
                    self._watch_job, job.metadata.name, configuration, client
                )
            return KubernetesWorkerResult(identifier=pid, status_code=status_code)

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: KubernetesWorkerJobConfiguration,
        grace_seconds: int = 30,
    ):
        """
        Stops a job for a cancelled flow run based on the provided infrastructure PID
        and run configuration.
        """
        await run_sync_in_worker_thread(
            self._stop_job, infrastructure_pid, configuration, grace_seconds
        )

    def _stop_job(
        self,
        infrastructure_pid: str,
        configuration: KubernetesWorkerJobConfiguration,
        grace_seconds: int = 30,
    ):
        client = self._get_configured_kubernetes_client(configuration)
        job_cluster_uid, job_namespace, job_name = self._parse_infrastructure_pid(
            infrastructure_pid
        )

        if job_namespace != configuration.namespace:
            raise InfrastructureNotAvailable(
                f"Unable to kill job {job_name!r}: The job is running in namespace "
                f"{job_namespace!r} but this worker expected jobs to be running in "
                f"namespace {configuration.namespace!r} based on the work pool and "
                "deployment configuration."
            )

        current_cluster_uid = self._get_cluster_uid(client)
        if job_cluster_uid != current_cluster_uid:
            raise InfrastructureNotAvailable(
                f"Unable to kill job {job_name!r}: The job is running on another "
                "cluster than the one specified by the infrastructure PID."
            )

        with self._get_batch_client(client) as batch_client:
            try:
                batch_client.delete_namespaced_job(
                    name=job_name,
                    namespace=job_namespace,
                    grace_period_seconds=grace_seconds,
                    # Foreground propagation deletes dependent objects before deleting
                    # owner objects. This ensures that the pods are cleaned up before
                    # the job is marked as deleted.
                    # See: https://kubernetes.io/docs/concepts/architecture/garbage-collection/#foreground-deletion # noqa
                    propagation_policy="Foreground",
                )
            except kubernetes.client.exceptions.ApiException as exc:
                if exc.status == 404:
                    raise InfrastructureNotFound(
                        f"Unable to kill job {job_name!r}: The job was not found."
                    ) from exc
                else:
                    raise

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

    def _parse_infrastructure_pid(
        self, infrastructure_pid: str
    ) -> Tuple[str, str, str]:
        """
        Parse a Kubernetes infrastructure PID into its component parts.

        Returns a cluster UID, namespace, and job name.
        """
        cluster_uid, namespace, job_name = infrastructure_pid.split(":", 2)
        return cluster_uid, namespace, job_name

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
