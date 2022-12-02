import pytest
from kubernetes.client.models import V1Deployment, V1Job, V1Pod, V1Service

from prefect_kubernetes.utilities import convert_manifest_to_model, parse_manifest_file

base_path = "tests/sample_k8s_resources"

sample_deployment_manifest = parse_manifest_file(f"{base_path}/sample_deployment.yaml")
sample_job_manifest = parse_manifest_file(f"{base_path}/sample_job.yaml")
sample_pod_manifest = parse_manifest_file(f"{base_path}/sample_pod.yaml")
sample_service_manifest = parse_manifest_file(f"{base_path}/sample_service.yaml")


@pytest.mark.parametrize(
    "manifest, model",
    [
        (sample_deployment_manifest, V1Deployment),
        (sample_job_manifest, V1Job),
        (sample_pod_manifest, V1Pod),
        (sample_service_manifest, V1Service),
    ],
)
def test_convert_manifest_to_model(manifest, model):
    v1_model = convert_manifest_to_model(manifest, model)

    assert isinstance(v1_model, model)

    assert v1_model.metadata.name == manifest["metadata"]["name"]
