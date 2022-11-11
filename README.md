# prefect-kubernetes

## Welcome!

Prefect integrations for interacting with Kubernetes resources.

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-kubernetes` with `pip`:

```bash
pip install prefect-kubernetes
```

### Write and run a flow
#### Generate a resource-specific client from `KubernetesClusterConfig`

```python
from prefect.blocks.kubernetes import KubernetesClusterConfig
from prefect_kubernetes.credentials import KubernetesCredentials

k8s_config = KubernetesClusterConfig.from_file('~/.kube/config')

k8s_credentials = KubernetesCredentials(cluster_config=k8s_config)

with k8s_credentials.get_client("core") as v1_core_client:
    for pod in v1_core_client.list_namespaced_pod('default').items:
        print(pod.metadata.name)
```

#### List jobs in a specific namespace

```python
from prefect import flow
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.jobs import list_namespaced_job

@flow
def kubernetes_orchestrator():
    v1_job_list = list_namespaced_job(
        kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
        namespace="my-namespace",
    )
```

#### Delete a pod using `V1DeleteOptions`

```python
from kubernetes.client.models import V1DeleteOptions

from prefect import flow
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.pods import delete_namespaced_pod

@flow
def kubernetes_orchestrator():
    v1_pod_list = delete_namespaced_pod(
        kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
        body=V1DeleteOptions(grace_period_seconds=42),
        namespace="my-namespace"
    )
```

## Resources

If you encounter any bugs while using `prefect-kubernetes`, feel free to open an issue in the [prefect-kubernetes](https://github.com/PrefectHQ/prefect-kubernetes) repository.

If you have any questions or issues while using `prefect-kubernetes`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-kubernetes` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-kubernetes.git

cd prefect-kubernetes/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
