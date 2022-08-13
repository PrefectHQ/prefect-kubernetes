import sys
from typing import Optional, Union

from kubernetes import client, config as kube_config
from kubernetes.config.config_exception import ConfigException
from prefect.blocks.kubernetes import KubernetesClusterConfig
from prefect_kubernetes.credentials import KubernetesApiKey

KubernetesClient = Union[client.BatchV1Api, client.CoreV1Api, client.AppsV1Api, client.ApiClient]

K8S_CLIENTS = {
    "job": client.BatchV1Api,
    "pod": client.CoreV1Api,
    "service": client.CoreV1Api,
    "deployment": client.AppsV1Api,
    "secret": client.CoreV1Api,
}

def get_kubernetes_client(
    resource: str,
    kubernetes_api_key: Optional[KubernetesApiKey] = None,
    kubernetes_cluster_config: Optional[KubernetesClusterConfig] = None
    
) -> KubernetesClient:
    """
    Utility function for loading kubernetes client object for a given resource.
    It will attempt to connect to a Kubernetes cluster in three steps with
    the first successful connection attempt becoming the mode of communication with a
    cluster.
    1. Attempt to use a KubernetesApiKey block containing a Kubernetes API Key. If
    `kubernetes_api_key` = `None` then it will attempt the next two connection
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
        - kubernetes_api_key (SecretStr): the value of a kubernetes api key in BearerToken format
    Returns:
        - KubernetesClient: an initialized and authenticated Kubernetes Client
    """
    if (resource or kubernetes_api_key) and kubernetes_cluster_config:
        raise ValueError("Please specify either a cluster config block or an API key to generate an API client")
    
    
    # KubernetesClusterConfig.get_api_client() returns a k8s api client that can be used to interact with any resource type
    if kubernetes_cluster_config:
        return kubernetes_cluster_config.get_api_client()
    
    client_type = K8S_CLIENTS[resource]

    if kubernetes_api_key:

        configuration = client.Configuration()
        configuration.api_key["authorization"] = kubernetes_api_key
        k8s_client = client_type(client.ApiClient(configuration))
    else:
        try:
            print("Trying to load in-cluster configuration...")
            kube_config.load_incluster_config()
        except ConfigException as exc:
            print("{} | Using out of cluster configuration option.".format(exc))
            print("Loading out-of-cluster configuration...")
            kube_config.load_kube_config()

        k8s_client = client_type()

    # if config.cloud.agent.kubernetes_keep_alive:
    #     _keep_alive(client=k8s_client)

    return k8s_client

def _keep_alive(client: KubernetesClient) -> None:
    """
    Setting the keep-alive flags on the kubernetes client object.
    Unfortunately neither the kubernetes library nor the urllib3 library which kubernetes is using
    internally offer the functionality to enable keep-alive messages. Thus the flags are added to
    be used on the underlying sockets.
    Args:
        - client (KubernetesClient): the kubernetes client object on which the keep-alive should be
            enabled
    """
    import socket

    socket_options = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]

    if hasattr(socket, "TCP_KEEPINTVL"):
        socket_options.append((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30))

    if hasattr(socket, "TCP_KEEPCNT"):
        socket_options.append((socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6))

    if hasattr(socket, "TCP_KEEPIDLE"):
        socket_options.append((socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 6))

    if sys.platform == "darwin":
        # TCP_KEEP_ALIVE not available on socket module in macOS, but defined in
        # https://github.com/apple/darwin-xnu/blob/2ff845c2e033bd0ff64b5b6aa6063a1f8f65aa32/bsd/netinet/tcp.h#L215
        TCP_KEEP_ALIVE = 0x10
        socket_options.append((socket.IPPROTO_TCP, TCP_KEEP_ALIVE, 30))

    client.api_client.rest_client.pool_manager.connection_pool_kw[
        "socket_options"
    ] = socket_options