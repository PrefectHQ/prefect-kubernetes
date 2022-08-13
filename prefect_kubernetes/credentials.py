from prefect.blocks.core import Block
from prefect_kubernetes.utilities import KubernetesClient, get_kubernetes_client
from pydantic import SecretStr

class KubernetesApiKey(Block):
    """Credentials block for API client generation across prefect-kubernetes tasks and flows.

    Args:
        api_key (SecretStr): API key to authenticate with the Kubernetes API. 
        
    Examples:
        Load a stored kubernetes API key:
        ```python
        from prefect_kubernetes import KubernetesApiKey
        
        kubernetes_api_key = KubernetesApiKey.load("my-k8s-api-key")
        ```
        
        Create a kubernetes API client from KubernetesApiKey and inferred cluster configuration:
        ```python
        from prefect_kubernetes import KubernetesApiKey
        from prefect_kubernetes.utilities import get_kubernetes_client
        
        kubernetes_api_key = KubernetesApiKey.load("my-k8s-api-key")
        kubernetes_api_client = get_kubernetes_client("pod", kubernetes_api_key)
        ```
        
        Create a namespaced kubernetes job:
        ```python
        from prefect_kubernetes import KubernetesApiKey
        from prefect_kubernetes.job import create_namespaced_job
        
        kubernetes_api_key = KubernetesApiKey.load("my-k8s-api-key")
        
        create_namespaced_job(
            namespace="default", body={"Marvin": "42"}, **kube_kwargs
        )
        ```
    """

    _block_type_name = "Kubernetes Api Key"
    _logo_url = "https://kubernetes-security.info/assets/img/logo.png?h=250"  # noqa
    
    api_key: SecretStr
    
    def get_client(self, resource: str) -> KubernetesClient:
        return get_kubernetes_client(resource, kubernetes_api_key=self.api_key)