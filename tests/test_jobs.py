import pytest
from kubernetes.client.exceptions import ApiValueError

from prefect_kubernetes.jobs import (
    create_namespaced_job,
    delete_namespaced_job,
    list_namespaced_job,
    patch_namespaced_job,
    read_namespaced_job,
    replace_namespaced_job,
)


async def test_invalid_body_raises_error(kubernetes_credentials):
    with pytest.raises(ApiValueError):
        await create_namespaced_job.fn(
            body=None, kubernetes_credentials=kubernetes_credentials
        )
    with pytest.raises(ApiValueError):
        await patch_namespaced_job.fn(
            body=None, job_name="", kubernetes_credentials=kubernetes_credentials
        )


async def test_create_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await create_namespaced_job.fn(
        body={"test": "a"},
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )

    assert _mock_api_batch_client.create_namespaced_job.call_args[1]["body"] == {
        "test": "a"
    }
    assert _mock_api_batch_client.create_namespaced_job.call_args[1]["a"] == "test"


async def test_delete_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await delete_namespaced_job.fn(
        job_name="test_job",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert (
        _mock_api_batch_client.delete_namespaced_job.call_args[1]["name"] == "test_job"
    )
    assert _mock_api_batch_client.delete_namespaced_job.call_args[1]["a"] == "test"


async def test_list_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await list_namespaced_job.fn(
        namespace="ns",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert _mock_api_batch_client.list_namespaced_job.call_args[1]["namespace"] == "ns"
    assert _mock_api_batch_client.list_namespaced_job.call_args[1]["a"] == "test"


async def test_patch_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await patch_namespaced_job.fn(
        body={"test": "a"},
        job_name="test_job",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert _mock_api_batch_client.patch_namespaced_job.call_args[1]["body"] == {
        "test": "a"
    }
    assert (
        _mock_api_batch_client.patch_namespaced_job.call_args[1]["name"] == "test_job"
    )
    assert _mock_api_batch_client.patch_namespaced_job.call_args[1]["a"] == "test"


async def test_read_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await read_namespaced_job.fn(
        job_name="test_job",
        namespace="ns",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert _mock_api_batch_client.read_namespaced_job.call_args[1]["name"] == "test_job"
    assert _mock_api_batch_client.read_namespaced_job.call_args[1]["namespace"] == "ns"
    assert _mock_api_batch_client.read_namespaced_job.call_args[1]["a"] == "test"


async def test_replace_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await replace_namespaced_job.fn(
        job_name="test_job",
        body={"test": "a"},
        namespace="ns",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert (
        _mock_api_batch_client.replace_namespaced_job.call_args[1]["name"] == "test_job"
    )
    assert (
        _mock_api_batch_client.replace_namespaced_job.call_args[1]["namespace"] == "ns"
    )
    assert _mock_api_batch_client.replace_namespaced_job.call_args[1]["body"] == {
        "test": "a"
    }
    assert _mock_api_batch_client.replace_namespaced_job.call_args[1]["a"] == "test"
