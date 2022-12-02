from unittest.mock import MagicMock

import pytest
from kubernetes.client.exceptions import ApiValueError
from kubernetes.client.models import V1Job
from prefect import flow

from prefect_kubernetes.jobs import (
    create_namespaced_job,
    delete_namespaced_job,
    list_namespaced_job,
    patch_namespaced_job,
    read_namespaced_job,
    replace_namespaced_job,
    run_namespaced_job,
)


async def test_null_body_raises_error(kubernetes_credentials):
    with pytest.raises(ApiValueError):
        await create_namespaced_job.fn(
            new_job=None, kubernetes_credentials=kubernetes_credentials
        )
    with pytest.raises(ApiValueError):
        await patch_namespaced_job.fn(
            job_updates=None, job_name="", kubernetes_credentials=kubernetes_credentials
        )
    with pytest.raises(ApiValueError):
        await replace_namespaced_job.fn(
            new_job=None, job_name="", kubernetes_credentials=kubernetes_credentials
        )


async def test_create_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await create_namespaced_job.fn(
        new_job=V1Job(metadata={"name": "test-job"}),
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )

    assert _mock_api_batch_client.create_namespaced_job.call_args[1][
        "body"
    ].metadata == {"name": "test-job"}
    assert _mock_api_batch_client.create_namespaced_job.call_args[1]["a"] == "test"


async def test_delete_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await delete_namespaced_job.fn(
        job_name="test-job",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert (
        _mock_api_batch_client.delete_namespaced_job.call_args[1]["name"] == "test-job"
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
        job_updates=V1Job(metadata={"name": "test-job"}),
        job_name="test-job",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert _mock_api_batch_client.patch_namespaced_job.call_args[1][
        "body"
    ].metadata == {"name": "test-job"}
    assert (
        _mock_api_batch_client.patch_namespaced_job.call_args[1]["name"] == "test-job"
    )
    assert _mock_api_batch_client.patch_namespaced_job.call_args[1]["a"] == "test"


async def test_read_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await read_namespaced_job.fn(
        job_name="test-job",
        namespace="ns",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert _mock_api_batch_client.read_namespaced_job.call_args[1]["name"] == "test-job"
    assert _mock_api_batch_client.read_namespaced_job.call_args[1]["namespace"] == "ns"
    assert _mock_api_batch_client.read_namespaced_job.call_args[1]["a"] == "test"


async def test_replace_namespaced_job(kubernetes_credentials, _mock_api_batch_client):
    await replace_namespaced_job.fn(
        job_name="test-job",
        new_job=V1Job(metadata={"name": "test-job"}),
        namespace="ns",
        a="test",
        kubernetes_credentials=kubernetes_credentials,
    )
    assert (
        _mock_api_batch_client.replace_namespaced_job.call_args[1]["name"] == "test-job"
    )
    assert (
        _mock_api_batch_client.replace_namespaced_job.call_args[1]["namespace"] == "ns"
    )
    assert _mock_api_batch_client.replace_namespaced_job.call_args[1][
        "body"
    ].metadata == {"name": "test-job"}
    assert _mock_api_batch_client.replace_namespaced_job.call_args[1]["a"] == "test"


async def test_run_namespaced_job_invalid_log_level_raises(
    kubernetes_credentials, _mock_api_batch_client
):
    @flow
    def test_flow():
        run_namespaced_job(
            kubernetes_credentials=kubernetes_credentials,
            job_to_run="tests/sample_k8s_resources/sample_job.yaml",
            log_level="invalid",
        )

    with pytest.raises(ValueError):
        test_flow()


async def test_run_namespaced_job_successful(
    kubernetes_credentials,
    _mock_api_batch_client,
    successful_job_status,
    mock_read_pod_log,
):
    _mock_api_batch_client.read_namespaced_job_status.return_value = MagicMock(
        return_value=successful_job_status
    )

    @flow
    def test_flow():
        return run_namespaced_job(
            kubernetes_credentials=kubernetes_credentials,
            job_to_run={"metadata": {"name": "success"}},
        )

    v1_job, pod_logs = test_flow()

    assert _mock_api_batch_client.create_namespaced_job.call_count == 1
    assert (
        _mock_api_batch_client.create_namespaced_job.call_args[1]["namespace"]
        == "default"
    )
    assert _mock_api_batch_client.create_namespaced_job.call_args[1]["body"] == {
        "metadata": {"name": "success"}
    }

    assert _mock_api_batch_client.read_namespaced_job_status.call_count == 1
    assert (
        _mock_api_batch_client.read_namespaced_job_status.call_args[1]["name"]
        == "success"
    )
    assert (
        _mock_api_batch_client.read_namespaced_job_status.call_args[1]["namespace"]
        == "default"
    )

    assert _mock_api_batch_client.delete_namespaced_job.call_count == 1
    assert (
        _mock_api_batch_client.delete_namespaced_job.call_args[1]["name"] == "success"
    )
    assert (
        _mock_api_batch_client.delete_namespaced_job.call_args[1]["namespace"]
        == "default"
    )
