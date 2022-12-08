from . import _version
from prefect_kubernetes.credentials import KubernetesCredentials  # noqa F401
from prefect_kubernetes.run_job import KubernetesRunJob, run_namespaced_job  # noqa F401


__version__ = _version.get_versions()["version"]
