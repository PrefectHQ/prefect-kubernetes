""" Utilities for working with the Python Kubernetes API. """
from pathlib import Path
from typing import TypeVar, Union

from kubernetes.client import models as k8s_models
from prefect.infrastructure.kubernetes import KubernetesJob, KubernetesManifest

# Note: `dict(str, str)` is the Kubernetes API convention for
# representing an OpenAPI `dict` with `str` keys and values.
base_types = {"str", "int", "float", "bool", "list[str]", "dict(str, str)", "object"}

V1KubernetesModel = TypeVar("V1KubernetesModel")


def convert_manifest_to_model(
    manifest: Union[Path, str, KubernetesManifest], v1_model_name: str
) -> V1KubernetesModel:
    """Recursively converts a `dict` representation of a Kubernetes resource to the
    corresponding Python model containing the Python models that compose it,
    according to the `openapi_types` on the class retrieved with `v1_model_name`.

    If `manifest` is a path-like object with a `.yaml` or `.yml` extension, it will be
    treated as a path to a Kubernetes resource manifest and loaded into a `dict`.

    Args:
        manifest: A path to a Kubernetes resource manifest or its `dict` representation.
        v1_model_name: The name of a Kubernetes client model to convert the manifest to.

    Returns:
        A populated instance of a Kubernetes client model with type `v1_model_name`.

    Raises:
        ValueError: If `v1_model_name` is not a valid Kubernetes client model name.
        ValueError: If `manifest` is path-like and is not a valid yaml filename.
    """
    if not manifest:
        return None

    if not (isinstance(v1_model_name, str) and v1_model_name in set(dir(k8s_models))):
        raise ValueError(
            "`v1_model` must be the name of a valid Kubernetes client model, received "
            f": {v1_model_name!r}"
        )

    if isinstance(manifest, (Path, str)):
        str_path = str(manifest)
        if not str_path.endswith((".yaml", ".yml")):
            raise ValueError("Manifest must be a valid dict or path to a .yaml file.")
        manifest = KubernetesJob.job_from_file(manifest)

    converted_manifest = {}
    v1_model = getattr(k8s_models, v1_model_name)
    valid_supplied_fields = (  # valid and specified fields for current `v1_model_name`
        (k, v)
        for k, v in v1_model.openapi_types.items()
        if v1_model.attribute_map[k] in manifest  # map goes 🐍 -> 🐫, user supplies 🐫
    )

    for field, value_type in valid_supplied_fields:
        if value_type.startswith("V1"):  # field value is another model
            converted_manifest[field] = convert_manifest_to_model(
                manifest[v1_model.attribute_map[field]], value_type
            )
        elif value_type.startswith("list[V1"):  # field value is a list of models
            field_item_type = value_type.replace("list[", "").replace("]", "")
            try:
                converted_manifest[field] = [
                    convert_manifest_to_model(item, field_item_type)
                    for item in manifest[v1_model.attribute_map[field]]
                ]
            except TypeError:
                converted_manifest[field] = manifest[v1_model.attribute_map[field]]
        elif value_type in base_types:  # field value is a primitive Python type
            converted_manifest[field] = manifest[v1_model.attribute_map[field]]

    return v1_model(**converted_manifest)
