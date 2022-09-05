from distutils.command.config import config
from typing import TYPE_CHECKING, Optional, Union

from kubernetes import client
from kubernetes import config as kube_config
from kubernetes.config.config_exception import ConfigException
from prefect.blocks.core import Block

# if TYPE_CHECKING:
from prefect.blocks.kubernetes import KubernetesClusterConfig
from pydantic import SecretStr

KubernetesClient = Union[
    client.BatchV1Api, client.CoreV1Api, client.AppsV1Api, client.ApiClient
]

K8S_CLIENTS = {
    "job": client.BatchV1Api,
    "core": client.CoreV1Api,
    "deployment": client.AppsV1Api,
}


class KubernetesCredentials(Block):
    """Credentials block for API client generation across prefect-kubernetes tasks and flows.

    Args:
        api_key (SecretStr): API key to authenticate with the Kubernetes API.

    Examples:
        Load a stored kubernetes API key:
        ```python
        from prefect_kubernetes.credentials import KubernetesCredentials

        kubernetes_credentials = KubernetesCredentials.load("my-k8s-credentials")
        ```

        Create a kubernetes API client from KubernetesCredentials and inferred cluster configuration:
        ```python
        from prefect_kubernetes import KubernetesCredentials
        from prefect_kubernetes.utilities import get_kubernetes_client

        kubernetes_credentials = KubernetesCredentials.load("my-k8s-api-key")
        kubernetes_api_client = kubernetes_credentials.get_core_client()
        ```

        Create a namespaced kubernetes job:
        ```python
        from prefect_kubernetes import KubernetesApiKey
        from prefect_kubernetes.job import create_namespaced_job

        kubernetes_credentials = KubernetesApiKey.load("my-k8s-api-key")

        create_namespaced_job(
            namespace="default", body={"Marvin": "42"}, **kube_kwargs
        )
        ```
    """

    _block_type_name = "Kubernetes Credentials"
    _logo_url = "https://kubernetes-security.info/assets/img/logo.png?h=250"  # noqa

    api_key: SecretStr = None
    cluster_config: KubernetesClusterConfig = None

    def get_core_client(self) -> client.CoreV1Api:
        """Convenience method for retrieving a kubernetes api client for core resources

        Returns:
            client.CoreV1Api: Kubernetes api client to interact with "pod", "service" and "secret" resources
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
            client.AppsV1Api: Kubernetes api client to interact with "deployment" resources
        """
        return self.get_kubernetes_client(resource="deployment")

    def get_kubernetes_client(self, resource: str) -> KubernetesClient:
        """
        Utility function for loading kubernetes client object for a given resource.
        It will attempt to connect to a Kubernetes cluster in three steps with
        the first successful connection attempt becoming the mode of communication with a
        cluster.
        1. Attempt to use a KubernetesCredentials block containing a Kubernetes API Key. If
        `kubernetes_credentials` = `None` then it will attempt the next two connection
        methods.
        2. Attempt in-cluster connection (will only work when running on a Pod in a cluster)
        3. Attempt out-of-cluster connection using the default location for a kube config file
        In some cases connections to the kubernetes server are dropped after being idle for some time
        (e.g. Azure Firewall drops idle connections after 4 minutes) which would result in
        ReadTimeoutErrors.
        In order to prevent that a periodic keep-alive message can be sent to the server to keep the
        connection open.
        Args:
            - resource (str): the name of the resource to retrieve a client for. Currently
                you can use one of these values: `job`, `pod`, `service`, `deployment`, `secret`

        Returns:
            - KubernetesClient: an initialized, authenticated Kubernetes Client
        """

        resource_specific_client = K8S_CLIENTS[resource]

        if self.api_key:
            configuration = client.Configuration()
            configuration.api_key["authorization"] = self.api_key.get_secret_value()
            configuration.api_key_prefix["authorization"] = "Bearer"
            k8s_client = resource_specific_client(
                api_client=client.ApiClient(configuration=configuration)
            )
        elif self.cluster_config:
            self.cluster_config.configure_client()
            k8s_client = resource_specific_client()
        else:
            try:
                print("Trying to load in-cluster configuration...")
                kube_config.load_incluster_config()
            except ConfigException as exc:
                print("{} | Using out of cluster configuration option.".format(exc))
                print("Loading out-of-cluster configuration...")
                kube_config.load_kube_config()

            k8s_client = resource_specific_client()

        return k8s_client
