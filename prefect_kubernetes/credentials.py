"""Module for managing Kubernetes credential configuration.
"""

from typing import TYPE_CHECKING, Optional, Union

from kubernetes import client
from kubernetes import config as kube_config
from kubernetes.config.config_exception import ConfigException
from prefect.blocks.core import Block

if TYPE_CHECKING:
    from prefect.blocks.kubernetes import KubernetesClusterConfig

KubernetesClient = Union[
    client.BatchV1Api, client.CoreV1Api, client.AppsV1Api, client.ApiClient
]

K8S_CLIENTS = {
    "job": client.BatchV1Api,
    "core": client.CoreV1Api,
    "deployment": client.AppsV1Api,
}


class KubernetesCredentials(Block):
    """Credentials block for generating configured Kubernetes API clients.

    Args:
        cluster_config (KubernetesClusterConfig, optional): a `KubernetesClusterConfig`
            block holding a JSON kube config for a specific kubernetes context


    Examples:
        Load stored Kubernetes credentials:
        ```python
        from prefect_kubernetes.credentials import KubernetesCredentials

        kubernetes_credentials = KubernetesCredentials.load("my-k8s-credentials")
        ```

        Create resource-specific API clients from KubernetesCredentials:
        ```python
        from prefect_kubernetes import KubernetesCredentials

        kubernetes_credentials = KubernetesCredentials.load("my-k8s-credentials")

        kubernetes_app_v1_client = kubernetes_credentials.get_app_client()
        kubernetes_batch_v1_client = kubernetes_credentials.get_batch_client()
        kubernetes_core_v1_client = kubernetes_credentials.get_core_client()
        ```

        Create a namespaced job:
        ```python
        from prefect_kubernetes import KubernetesCredentials
        from prefect_kubernetes.job import create_namespaced_job

        kubernetes_credentials = KubernetesCredentials.load("my-k8s-credentials")

        create_namespaced_job(
            body={"Marvin": "42"}, kubernetes_credentials=kubernetes_credentials
        )
        ```
    """

    _block_type_name = "Kubernetes Credentials"
    _logo_url = "https://kubernetes-security.info/assets/img/logo.png?h=250"  # noqa

    cluster_config: Optional[KubernetesClusterConfig] = None

    def get_core_client(self) -> client.CoreV1Api:
        """Convenience method for retrieving a kubernetes api client for core resources

        Returns:
            client.CoreV1Api: Kubernetes api client to interact with "pod", "service"
            and "secret" resources
        """
        return self.get_kubernetes_client(resource="core")

    def get_batch_client(self) -> client.BatchV1Api:
        """Convenience method for retrieving a kubernetes api client for job resources

        Returns:
            client.BatchV1Api: Kubernetes api client to interact with "job" resources
        """
        return self.get_kubernetes_client(resource="job")

    def get_app_client(self) -> client.AppsV1Api:
        """Convenience method for retrieving a kubernetes api client for deployment resources

        Returns:
            client.AppsV1Api: Kubernetes client to interact with "deployment" resources
        """
        return self.get_kubernetes_client(resource="deployment")

    def get_kubernetes_client(self, resource: str) -> KubernetesClient:
        """
        Utility function for loading kubernetes client object for a given resource.
        It will attempt to connect to a Kubernetes cluster in three steps with the
        first successful connection attempt becoming the mode of communication with a
        cluster.

        1. It will first attempt to use `KubernetesCredentials` block's `cluster_config`
        to configure a client using `KubernetesClusterConfig.configure_client` and then
        return the `resource_specific_client`.
        2. Attempt in-cluster connection (will only work when running within a cluster)
        3. Attempt out-of-cluster connection using the default kube config path
        In some cases connections to the kubernetes server are dropped after being idle
        for some time (e.g. Azure Firewall drops idle connections after 4 minutes) which
        would result in ReadTimeoutErrors.

        In order to prevent that a periodic keep-alive message can be sent to the server
        to keep the connection open.

        Args:
            - resource (str): the name of the resource to retrieve a client for.
                Currently you can use one of these values:
                `job`, `pod`, `service`, `deployment`, `secret`

        Returns:
            - KubernetesClient: an initialized, configured Kubernetes Client
        """

        resource_specific_client = K8S_CLIENTS[resource]

        if self.cluster_config:
            self.cluster_config.configure_client()
            return resource_specific_client()
        else:
            try:
                print("Trying to load in-cluster configuration...")
                kube_config.load_incluster_config()
            except ConfigException as exc:
                print("{} | Using out of cluster configuration option.".format(exc))
                print("Loading out-of-cluster configuration...")
                kube_config.load_kube_config()

            return resource_specific_client()
