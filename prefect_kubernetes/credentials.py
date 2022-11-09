"""Module for defining Kubernetes credential handling and client generation."""

from contextlib import contextmanager
from typing import Generator, Optional

from kubernetes import config as kube_config
from kubernetes.client import ApiClient, AppsV1Api, BatchV1Api, CoreV1Api
from kubernetes.config.config_exception import ConfigException
from prefect.blocks.core import Block
from prefect.blocks.kubernetes import KubernetesClusterConfig


class KubernetesCredentials(Block):
    """Credentials block for generating configured Kubernetes API clients.

    Attributes:
        cluster_config: A `KubernetesClusterConfig` block holding a JSON kube
            config for a specific kubernetes context.

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
        from prefect import flow
        from prefect_kubernetes import KubernetesCredentials
        from prefect_kubernetes.job import create_namespaced_job

        from kubernetes.client.models import V1Job

        kubernetes_credentials = KubernetesCredentials.load("my-k8s-credentials")

        @flow
        def kubernetes_orchestrator():
            create_namespaced_job(
                body=V1Job(**{"Marvin": "42"}),
                kubernetes_credentials=kubernetes_credentials
            )
        ```
    """

    _block_type_name = "Kubernetes Credentials"
    _logo_url = "https://images.ctfassets.net/zscdif0zqppk/oYuHjIbc26oilfQSEMjRv/a61f5f6ef406eead2df5231835b4c4c2/logo.png?h=250"  # noqa

    cluster_config: Optional[KubernetesClusterConfig] = None

    @contextmanager
    def get_app_client(self) -> Generator[AppsV1Api, None, None]:
        """Convenience method for retrieving a kubernetes api client for deployment resources

        Returns:
            Kubernetes api client generator to interact with "deployment" resources.
        """
        generic_client = self.get_kubernetes_client()
        try:
            yield AppsV1Api(api_client=generic_client)
        finally:
            generic_client.rest_client.pool_manager.clear()

    @contextmanager
    def get_batch_client(self) -> Generator[BatchV1Api, None, None]:
        """Convenience method for retrieving a kubernetes api client for job resources.

        Returns:
            Kubernetes api client generator to interact with "job" resources.
        """
        generic_client = self.get_kubernetes_client()
        try:
            yield BatchV1Api(api_client=generic_client)
        finally:
            generic_client.rest_client.pool_manager.clear()

    @contextmanager
    def get_core_client(self) -> Generator[CoreV1Api, None, None]:
        """Convenience method for retrieving a kubernetes api client for core resources.

        Returns:
            Kubernetes api client generator to interact with "pod", "service"
            and "secret" resources.
        """
        generic_client = self.get_kubernetes_client()
        try:
            yield CoreV1Api(api_client=generic_client)
        finally:
            generic_client.rest_client.pool_manager.clear()

    def get_kubernetes_client(self) -> ApiClient:
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

        Returns:
            An authenticated, generic Kubernetes Client.
        """

        with ApiClient() as client:
            if self.cluster_config:
                self.cluster_config.configure_client()
                return client
            else:
                try:
                    kube_config.load_incluster_config()
                except ConfigException:
                    kube_config.load_kube_config()

                return client
