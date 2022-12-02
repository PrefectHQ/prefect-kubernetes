""" Utilities for working with the Python Kubernetes API. """
import yaml
from kubernetes.client import models as k8s_models
from prefect.infrastructure.kubernetes import KubernetesManifest

base_types = {"str", "int", "float", "bool", "list[str]", "dict(str, str)"}


def parse_manifest_file(manifest_file: str) -> KubernetesManifest:
    """Parse a Kubernetes manifest file into a Python object.

    Args:
        manifest_file: A path to a Kubernetes manifest file.

    Returns:
        A `dict` representation of the Kubernetes manifest.
    """
    with open(manifest_file, "r") as f:
        yaml_dict = yaml.safe_load(f)

    return yaml_dict


def convert_manifest_to_model(manifest: KubernetesManifest, v1_model: object) -> object:
    """Recursively converts a `dict` representation of a Kubernetes resource to the
    corresponding Python model containing the Python models that compose it,
    according to the `openapi_types` on the given `model` class.

    Args:
        manifest: A `dict` representation of a Kubernetes resource.

    Returns:
        A Kubernetes client model of type `model`.
    """
    converted_manifest = {}

    if isinstance(manifest, str):
        return manifest

    for key, value_type_name in v1_model.openapi_types.items():
        if v1_model.attribute_map[key] not in manifest:
            continue
        elif value_type_name.startswith("V1"):
            value_type = getattr(k8s_models, value_type_name)
            converted_manifest[key] = convert_manifest_to_model(
                manifest[key], value_type
            )
        elif value_type_name.startswith("list[V1"):
            value_item_type_name = value_type_name.replace("list[", "").replace("]", "")
            value_type = getattr(k8s_models, value_item_type_name)
            converted_manifest[key] = [
                convert_manifest_to_model(item, value_type) for item in manifest[key]
            ]
        elif value_type_name in base_types:
            converted_manifest[key] = manifest.get(v1_model.attribute_map[key])

    return v1_model(**converted_manifest)
