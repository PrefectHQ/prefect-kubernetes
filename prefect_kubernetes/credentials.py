"""Module for defining Kubernetes credential handling and client generation."""

from contextlib import contextmanager
from typing import Generator, Optional, Union

from kubernetes import config as kube_config
from kubernetes.client import (
    ApiClient,
    AppsV1Api,
    BatchV1Api,
    Configuration,
    CoreV1Api,
    CustomObjectsApi,
)
from kubernetes.config.config_exception import ConfigException
from prefect.blocks.core import Block
from prefect.blocks.kubernetes import KubernetesClusterConfig
from prefect.utilities.collections import listrepr
from typing_extensions import Literal

KubernetesClient = Union[AppsV1Api, BatchV1Api, CoreV1Api]

K8S_CLIENT_TYPES = {
    "apps": AppsV1Api,
    "batch": BatchV1Api,
    "core": CoreV1Api,
    "custom_objects": CustomObjectsApi,
}


class KubernetesCredentials(Block):
    """Credentials block for generating configured Kubernetes API clients.

    Attributes:
        cluster_config: A `KubernetesClusterConfig` block holding a JSON kube
            config for a specific kubernetes context.

    Example:
        Load stored Kubernetes credentials:
        ```python
        from prefect_kubernetes.credentials import KubernetesCredentials

        kubernetes_credentials = KubernetesCredentials.load("BLOCK_NAME")
        ```
    """

    _block_type_name = "Kubernetes Credentials"
    _logo_url = "https://images.ctfassets.net/zscdif0zqppk/oYuHjIbc26oilfQSEMjRv/a61f5f6ef406eead2df5231835b4c4c2/logo.png?h=250"  # noqa
    _documentation_url = "https://prefecthq.github.io/prefect-kubernetes/credentials/#prefect_kubernetes.credentials.KubernetesCredentials"  # noqa

    cluster_config: Optional[KubernetesClusterConfig] = None

    @contextmanager
    def get_client(
        self,
        client_type: Literal["apps", "batch", "core", "custom_objects"],
        configuration: Optional[Configuration] = None,
    ) -> Generator[KubernetesClient, None, None]:
        """Convenience method for retrieving a Kubernetes API client for deployment resources.

        Args:
            client_type: The resource-specific type of Kubernetes client to retrieve.

        Yields:
            An authenticated, resource-specific Kubernetes API client.

        Example:
            ```python
            from prefect_kubernetes.credentials import KubernetesCredentials

            with KubernetesCredentials.get_client("core") as core_v1_client:
                for pod in core_v1_client.list_namespaced_pod():
                    print(pod.metadata.name)
            ```
        """
        client_config = configuration or Configuration()

        with ApiClient(configuration=client_config) as generic_client:
            try:
                yield self.get_resource_specific_client(client_type)
            finally:
                generic_client.rest_client.pool_manager.clear()

    def get_resource_specific_client(
        self,
        client_type: str,
    ) -> Union[AppsV1Api, BatchV1Api, CoreV1Api]:
        """
        Utility function for configuring a generic Kubernetes client.
        It will attempt to connect to a Kubernetes cluster in three steps with
        the first successful connection attempt becoming the mode of communication with
        a cluster:

        1. It will first attempt to use a `KubernetesCredentials` block's
        `cluster_config` to configure a client using
        `KubernetesClusterConfig.configure_client`.

        2. Attempt in-cluster connection (will only work when running on a pod).

        3. Attempt out-of-cluster connection using the default location for a
        kube config file.

        Args:
            client_type: The Kubernetes API client type for interacting with specific
                Kubernetes resources.

        Returns:
            KubernetesClient: An authenticated, resource-specific Kubernetes Client.

        Raises:
            ValueError: If `client_type` is not a valid Kubernetes API client type.
        """

        if self.cluster_config:
            self.cluster_config.configure_client()
        else:
            try:
                kube_config.load_incluster_config()
            except ConfigException:
                kube_config.load_kube_config()

        try:
            return K8S_CLIENT_TYPES[client_type]()
        except KeyError:
            raise ValueError(
                f"Invalid client type provided '{client_type}'."
                f" Must be one of {listrepr(K8S_CLIENT_TYPES.keys())}."
            )
