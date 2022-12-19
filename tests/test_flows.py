import pytest

from prefect_kubernetes.flows import run_namespaced_job


async def test_run_namespaced_job_successful(
    valid_kubernetes_job_block, _mock_api_batch_client, successful_job_status
):
    _mock_api_batch_client.read_namespaced_job_status.return_value = (
        successful_job_status
    )

    await run_namespaced_job(kubernetes_job=valid_kubernetes_job_block)

    assert _mock_api_batch_client.create_namespaced_job.call_count == 1
    assert (
        _mock_api_batch_client.create_namespaced_job.call_args[1]["namespace"]
        == "default"
    )
    assert (
        _mock_api_batch_client.create_namespaced_job.call_args[1]["body"].metadata.name
        == "pi"
    )

    assert _mock_api_batch_client.read_namespaced_job_status.call_count == 1

    assert _mock_api_batch_client.delete_namespaced_job.call_count == 1


async def test_run_namespaced_job_successful_no_delete_after_completion(
    valid_kubernetes_job_block, _mock_api_batch_client, successful_job_status
):
    _mock_api_batch_client.read_namespaced_job_status.return_value = (
        successful_job_status
    )

    valid_kubernetes_job_block.delete_after_completion = False

    await run_namespaced_job(kubernetes_job=valid_kubernetes_job_block)

    assert _mock_api_batch_client.create_namespaced_job.call_count == 1
    assert (
        _mock_api_batch_client.create_namespaced_job.call_args[1]["namespace"]
        == "default"
    )
    assert (
        _mock_api_batch_client.create_namespaced_job.call_args[1]["body"].metadata.name
        == "pi"
    )

    assert _mock_api_batch_client.read_namespaced_job_status.call_count == 1

    assert _mock_api_batch_client.delete_namespaced_job.call_count == 0


async def test_run_namespaced_job_unsuccessful(
    valid_kubernetes_job_block, _mock_api_batch_client, successful_job_status
):

    successful_job_status.status.failed = 1
    successful_job_status.status.succeeded = None
    _mock_api_batch_client.read_namespaced_job_status.return_value = (
        successful_job_status
    )

    with pytest.raises(RuntimeError, match="failed, check the Kubernetes pod logs"):
        await run_namespaced_job(kubernetes_job=valid_kubernetes_job_block)

    assert _mock_api_batch_client.create_namespaced_job.call_count == 1
    assert (
        _mock_api_batch_client.create_namespaced_job.call_args[1]["namespace"]
        == "default"
    )
    assert (
        _mock_api_batch_client.create_namespaced_job.call_args[1]["body"].metadata.name
        == "pi"
    )

    assert _mock_api_batch_client.read_namespaced_job_status.call_count == 1

    assert _mock_api_batch_client.delete_namespaced_job.call_count == 0


async def test_run_namespaced_job_successful_with_logging(
    valid_kubernetes_job_block,
    _mock_api_batch_client,
    successful_job_status,
    mock_list_namespaced_pod,
    read_pod_logs,
):
    _mock_api_batch_client.read_namespaced_job_status.return_value = (
        successful_job_status
    )

    valid_kubernetes_job_block.log_level = "INFO"

    await run_namespaced_job(kubernetes_job=valid_kubernetes_job_block)

    assert _mock_api_batch_client.create_namespaced_job.call_count == 1
    assert (
        _mock_api_batch_client.create_namespaced_job.call_args[1]["namespace"]
        == "default"
    )
    assert (
        _mock_api_batch_client.create_namespaced_job.call_args[1]["body"].metadata.name
        == "pi"
    )

    assert _mock_api_batch_client.read_namespaced_job_status.call_count == 1

    assert read_pod_logs.call_count == 1

    assert _mock_api_batch_client.delete_namespaced_job.call_count == 1
