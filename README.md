# prefect-kubernetes

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-kubernetes/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-kubernetes?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-kubernetes/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-kubernetes?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-kubernetes/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-kubernetes?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-kubernetes/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-kubernetes?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>


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

Then, register to [view the block](https://orion-docs.prefect.io/ui/blocks/) on Prefect Cloud:

```bash
prefect block register -m prefect_kubernetes.credentials
```

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).


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
    v1_pod = delete_namespaced_pod(
        kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
        pod_name="my-pod-to-delete",
        delete_options=V1DeleteOptions(grace_period_seconds=42),
        namespace="my-namespace"
    )
```
#### Patch an existing deployment

```python
from kubernetes.client.models import V1Deployment

from prefect import flow
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.deployments import patch_namespaced_deployment

@flow
def kubernetes_orchestrator():
    v1_deployment = patch_namespaced_deployment(
        kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
        deployment_name="my-deployment",
        deployment_updates=V1Deployment(spec={"replicas": 2}),
        namespace="my-namespace"
    )
```
### List services in a namespace
```python
from prefect import flow
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.services import list_namespaced_service

@flow
def kubernetes_orchestrator():
    v1_service_list = list_namespaced_service(
        kubernetes_credentials=KubernetesCredentials.load("k8s-creds"),
        namespace="my-namespace",
    )
```

## Resources

If you encounter any bugs while using `prefect-kubernetes`, feel free to open an issue in the [prefect-kubernetes](https://github.com/PrefectHQ/prefect-kubernetes) repository.

If you have any questions or issues while using `prefect-kubernetes`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to ⭐️ or watch [`prefect-kubernetes`](https://github.com/PrefectHQ/prefect-kubernetes) for updates too!

## Development

If you'd like to install a version of `prefect-kubernetes` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-kubernetes.git

cd prefect-kubernetes/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
