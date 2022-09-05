from kubernetes.client.exceptions import ApiException


class KubernetesJobDefinitionError(Exception):
    """An exception to raise when a Kubernetes job definition is invalid"""


class KubernetesJobFailedError(Exception):
    """An exception to raise when a job orchestrated by a prefect_kubernetes task fails"""


class KubernetesResourceNotFoundError(ApiException):
    """An exception to raise when a Kubernetes resource cannot be found by an api client"""
