import pytest
from kubernetes.client import AppsV1Api, BatchV1Api, CoreV1Api


@pytest.mark.parametrize(
    "resource_type_method,client_type",
    [
        ("get_app_client", AppsV1Api),
        ("get_batch_client", BatchV1Api),
        ("get_core_client", CoreV1Api),
    ],
)
def test_client_return_type(kubernetes_credentials, resource_type_method, client_type):
    resource_specific_client = getattr(kubernetes_credentials, resource_type_method)

    with resource_specific_client() as client:
        assert isinstance(client, client_type)
