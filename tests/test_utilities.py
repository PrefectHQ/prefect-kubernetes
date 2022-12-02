import pytest
from kubernetes.client import models as k8s_models
from prefect.infrastructure.kubernetes import KubernetesJob

from prefect_kubernetes.utilities import convert_manifest_to_model

base_path = "tests/sample_k8s_resources"

sample_deployment_manifest = KubernetesJob.job_from_file(
    f"{base_path}/sample_deployment.yaml"
)
sample_job_manifest = KubernetesJob.job_from_file(f"{base_path}/sample_job.yaml")
sample_pod_manifest = KubernetesJob.job_from_file(f"{base_path}/sample_pod.yaml")
sample_service_manifest = KubernetesJob.job_from_file(
    f"{base_path}/sample_service.yaml"
)


@pytest.mark.parametrize(
    "manifest, model",
    [
        (sample_deployment_manifest, "V1Deployment"),
        (sample_job_manifest, "V1Job"),
        (sample_pod_manifest, "V1Pod"),
        (sample_service_manifest, "V1Service"),
    ],
)
def test_convert_manifest_to_model(manifest, model):
    v1_model = convert_manifest_to_model(manifest, model)

    assert isinstance(v1_model, getattr(k8s_models, model))

    assert isinstance(v1_model.metadata, getattr(k8s_models, "V1ObjectMeta"))

    assert v1_model.metadata.name == manifest["metadata"]["name"]

    # models' fields default to None if not specified in the manifest
    assert {k: v for k, v in v1_model.metadata.to_dict().items() if v} == manifest[
        "metadata"
    ]


def test_bad_manifest_filename_raises():
    with pytest.raises(
        ValueError, match="Manifest must be a valid dict or path to a .yaml file."
    ):
        convert_manifest_to_model("isaid86753OHnahhhEEEIIIEEEYEN", "V1Deployment")


def test_bad_model_type_raises():
    with pytest.raises(
        ValueError,
        match="`v1_model` must be the name of a valid Kubernetes client model.",
    ):
        convert_manifest_to_model(sample_deployment_manifest, ["V1Deployment"])
