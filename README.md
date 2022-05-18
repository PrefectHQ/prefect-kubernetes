# prefect-kubernetes

## Welcome!

Prefect integrations for interacting with prefect-kubernetes.

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

```python
from prefect import flow
from prefect_kubernetes.tasks import (
    goodbye_prefect_kubernetes,
    hello_prefect_kubernetes,
)


@flow
def example_flow():
    hello_prefect_kubernetes
    goodbye_prefect_kubernetes

example_flow()
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
