""" Utilities for working with the Python Kubernetes API. """
from pathlib import Path
from typing import Optional, TypeVar, Union

from kubernetes.client import models as k8s_models
from prefect.infrastructure.kubernetes import KubernetesJob, KubernetesManifest
from slugify import slugify

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


def _slugify_name(name: str, max_length: int = 45) -> Optional[str]:
    """
    Slugify text for use as a name.

    Keeps only alphanumeric characters and dashes, and caps the length
    of the slug at 45 chars.

    The 45 character length allows room for the k8s utility
    "generateName" to generate a unique name from the slug while
    keeping the total length of a name below 63 characters, which is
    the limit for e.g. label names that follow RFC 1123 (hostnames) and
    RFC 1035 (domain names).

    Args:
        name: The name of the job

    Returns:
        The slugified job name or None if the slugified name is empty
    """
    slug = slugify(
        name,
        max_length=max_length,  # Leave enough space for generateName
        regex_pattern=r"[^a-zA-Z0-9-]+",
    )

    return slug if slug else None


def _slugify_label_key(key: str, max_length: int = 63, prefix_max_length=253) -> str:
    """
    Slugify text for use as a label key.

    Keys are composed of an optional prefix and name, separated by a slash (/).

    Keeps only alphanumeric characters, dashes, underscores, and periods.
    Limits the length of the label prefix to 253 characters.
    Limits the length of the label name to 63 characters.

    See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

    Args:
        key: The label key

    Returns:
        The slugified label key
    """  # noqa
    if "/" in key:
        prefix, name = key.split("/", maxsplit=1)
    else:
        prefix = None
        name = key

    name_slug = (
        slugify(name, max_length=max_length, regex_pattern=r"[^a-zA-Z0-9-_.]+").strip(
            "_-."  # Must start or end with alphanumeric characters
        )
        or name
    )
    # Fallback to the original if we end up with an empty slug, this will allow
    # Kubernetes to throw the validation error

    if prefix:
        prefix_slug = (
            slugify(
                prefix,
                max_length=prefix_max_length,
                regex_pattern=r"[^a-zA-Z0-9-\.]+",
            ).strip(
                "_-."
            )  # Must start or end with alphanumeric characters
            or prefix
        )

        return f"{prefix_slug}/{name_slug}"

    return name_slug


def _slugify_label_value(value: str, max_length: int = 63) -> str:
    """
    Slugify text for use as a label value.

    Keeps only alphanumeric characters, dashes, underscores, and periods.
    Limits the total length of label text to below 63 characters.

    See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

    Args:
        value: The text for the label

    Returns:
        The slugified value
    """  # noqa
    slug = (
        slugify(value, max_length=max_length, regex_pattern=r"[^a-zA-Z0-9-_\.]+").strip(
            "_-."  # Must start or end with alphanumeric characters
        )
        or value
    )
    # Fallback to the original if we end up with an empty slug, this will allow
    # Kubernetes to throw the validation error

    return slug
