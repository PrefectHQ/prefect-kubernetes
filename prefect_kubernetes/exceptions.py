class KubernetesJobDefinitionError(Exception):
    """An exception to raise when a Kubernetes job definition is invalid

    """
    
class KubernetesJobFailedError(Exception):
    """An exception to raise when a job orchestrated by a prefect_kubernetes task fails

    """