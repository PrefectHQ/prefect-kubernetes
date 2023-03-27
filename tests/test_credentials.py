import pytest
from kubernetes.client import AppsV1Api, BatchV1Api, CoreV1Api, CustomObjectsApi


@pytest.mark.parametrize(
    "resource_type,client_type",
    [
        ("apps", AppsV1Api),
        ("batch", BatchV1Api),
        ("core", CoreV1Api),
        ("custom_objects", CustomObjectsApi),
    ],
)
def test_client_return_type(kubernetes_credentials, resource_type, client_type):
    with kubernetes_credentials.get_client(resource_type) as client:
        assert isinstance(client, client_type)


def test_client_bad_resource_type(kubernetes_credentials):
    with pytest.raises(
        ValueError, match="Invalid client type provided 'shoo-ba-daba-doo'"
    ):
        with kubernetes_credentials.get_client("shoo-ba-daba-doo"):
            pass
