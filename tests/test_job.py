import pytest
from prefect import flow

from prefect_kubernetes import exceptions as err
from prefect_kubernetes.job import (
    create_namespaced_job,
    delete_namespaced_job,
    list_namespaced_job,
    patch_namespaced_job,
    read_namespaced_job,
    replace_namespaced_job,
    run_namespaced_job,
)


async def test_invalid_body_raises_error(kubernetes_api_key):
    with pytest.raises(err.KubernetesJobDefinitionError):
        await create_namespaced_job.fn(body=None, kubernetes_api_key=kubernetes_api_key)
    with pytest.raises(err.KubernetesJobDefinitionError):
        await patch_namespaced_job.fn(
            body=None, job_name="", kubernetes_api_key=kubernetes_api_key
        )


async def test_create_namespaced_job(kubernetes_api_key, api_batch_client):
    await create_namespaced_job.fn(
        body={"test": "a"},
        kube_kwargs={"a": "test"},
        kubernetes_api_key=kubernetes_api_key,
    )

    assert api_batch_client.create_namespaced_job.call_args[1]["body"] == {"test": "a"}
    assert api_batch_client.create_namespaced_job.call_args[1]["a"] == "test"


async def test_delete_namespaced_job(kubernetes_api_key, api_batch_client):
    await delete_namespaced_job.fn(
        job_name="test_job",
        kube_kwargs={"a": "test"},
        kubernetes_api_key=kubernetes_api_key,
    )
    assert api_batch_client.delete_namespaced_job.call_args[1]["name"] == "test_job"
    assert api_batch_client.delete_namespaced_job.call_args[1]["a"] == "test"


async def test_list_namespaced_job(kubernetes_api_key, api_batch_client):
    await list_namespaced_job.fn(
        namespace="ns",
        kube_kwargs={"a": "test"},
        kubernetes_api_key=kubernetes_api_key,
    )
    assert api_batch_client.list_namespaced_job.call_args[1]["namespace"] == "ns"
    assert api_batch_client.list_namespaced_job.call_args[1]["a"] == "test"


async def test_patch_namespaced_job(kubernetes_api_key, api_batch_client):
    await patch_namespaced_job.fn(
        body={"test": "a"},
        job_name="test_job",
        kube_kwargs={"a": "test"},
        kubernetes_api_key=kubernetes_api_key,
    )
    assert api_batch_client.patch_namespaced_job.call_args[1]["body"] == {"test": "a"}
    assert api_batch_client.patch_namespaced_job.call_args[1]["name"] == "test_job"
    assert api_batch_client.patch_namespaced_job.call_args[1]["a"] == "test"


async def test_read_namespaced_job(kubernetes_api_key, api_batch_client):
    await read_namespaced_job.fn(
        job_name="test_job",
        namespace="ns",
        kube_kwargs={"a": "test"},
        kubernetes_api_key=kubernetes_api_key,
    )
    assert api_batch_client.read_namespaced_job.call_args[1]["name"] == "test_job"
    assert api_batch_client.read_namespaced_job.call_args[1]["namespace"] == "ns"
    assert api_batch_client.read_namespaced_job.call_args[1]["a"] == "test"


async def test_replace_namespaced_job(kubernetes_api_key, api_batch_client):
    await replace_namespaced_job.fn(
        job_name="test_job",
        body={"test": "a"},
        namespace="ns",
        kube_kwargs={"a": "test"},
        kubernetes_api_key=kubernetes_api_key,
    )
    assert api_batch_client.replace_namespaced_job.call_args[1]["name"] == "test_job"
    assert api_batch_client.replace_namespaced_job.call_args[1]["namespace"] == "ns"
    assert api_batch_client.replace_namespaced_job.call_args[1]["body"] == {"test": "a"}
    assert api_batch_client.replace_namespaced_job.call_args[1]["a"] == "test"


# async def test_run_namespaced_job(
#     kubernetes_api_key, api_batch_client, api_core_client
# ):
#     @flow
#     def test_flow():
#         run_namespaced_job(
#             body={"metadata": {"name": "test"}},
#             namespace="ns",
#             kube_kwargs={"a": "test"},
#             kubernetes_api_key=kubernetes_api_key,
#             log_level="DEBUG",
#         )

#     test_flow()

#     assert api_batch_client.create_namespaced_job.call_args[1]["body"] == {"test": "a"}
#     assert api_batch_client.create_namespaced_job.call_args[1]["namespace"] == "ns"
#     assert api_batch_client.create_namespaced_job.call_args[1]["a"] == "test"

#     assert api_batch_client.read_namespaced_job_status.call_args[1]["body"] == {
#         "test": "a"
#     }
#     assert api_batch_client.read_namespaced_job_status.call_args[1]["namespace"] == "ns"

#     assert api_core_client.list_namespaced_pod.call_args[1]["namespace"] == "ns"
