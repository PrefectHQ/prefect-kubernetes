from . import _version
from prefect_kubernetes.credentials import KubernetesCredentials  # noqa F401

__version__ = _version.get_versions()["version"]
