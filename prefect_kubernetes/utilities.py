""" Utilities for working with the Python Kubernetes API. """
from pathlib import Path
from typing import TypeVar, Union

from kubernetes.client import models as k8s_models
from prefect.infrastructure.kubernetes import KubernetesJob, KubernetesManifest

base_types = {"str", "int", "float", "bool", "list[str]", "dict(str, str)"}

V1Model = TypeVar("V1Model")


def convert_manifest_to_model(
    manifest: Union[Path, KubernetesManifest], v1_model_name: str
) -> V1Model:
    """Recursively converts a `dict` representation of a Kubernetes resource to the
    corresponding Python model containing the Python models that compose it,
    according to the `openapi_types` on the class retrieved with `v1_model_name`.

    Args:
        manifest: A path to a Kubernetes resource manifest or its `dict` representation.
        v1_model_name: The name of a Kubernetes client model to convert the manifest to.

    Returns:
        A populated instance of a Kubernetes client model with type `v1_model_name`.

    Raises:
        ValueError: If the given `v1_model_name` is not a Kubernetes client model name.
        ValueError: If the given `manifest` is path-like and is a valid yaml filename.
    """

    if v1_model_name not in dir(k8s_models):
        raise ValueError(
            "`v1_model` must be the name of a valid Kubernetes client model."
        )

    if isinstance(manifest, (Path, str)):
        str_path = str(manifest)
        if not (str_path.endswith(".yaml") or str_path.endswith(".yml")):
            raise ValueError("Manifest must be a valid dict or path to a .yaml file.")
        manifest = KubernetesJob.job_from_file(manifest)

    converted_manifest = {}
    v1_model = getattr(k8s_models, v1_model_name)

    for field, value_type in v1_model.openapi_types.items():
        if v1_model.attribute_map[field] not in manifest:
            continue
        elif value_type.startswith("V1"):
            converted_manifest[field] = convert_manifest_to_model(
                manifest[field], value_type
            )
        elif value_type.startswith("list[V1"):
            field_item_type = value_type.replace("list[", "").replace("]", "")
            converted_manifest[field] = [
                convert_manifest_to_model(item, field_item_type)
                for item in manifest[field]
            ]
        elif value_type in base_types:
            converted_manifest[field] = manifest[v1_model.attribute_map[field]]

    return v1_model(**converted_manifest)
