"""Module to define common exceptions within `prefect_kubernetes`."""

from kubernetes.client.exceptions import ApiException


class KubernetesJobDefinitionError(Exception):
    """An exception for when a Kubernetes job definition is invalid"""


class KubernetesJobFailedError(Exception):
    """An exception for when a Kubernetes job fails"""


class KubernetesResourceNotFoundError(ApiException):
    """An exception for when a Kubernetes resource cannot be found by a client"""
