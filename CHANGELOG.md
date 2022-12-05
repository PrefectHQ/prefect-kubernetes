# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `run_namespaced_job` task allowing easy execution of a well-specified job on a cluster specified by
    a `KubernetesCredentials` block - [#28](https://github.com/PrefectHQ/prefect-kubernetes/pull/28)

- `convert_manifest_to_model` utility function for converting a Kubernetes manifest to a model object
    - [#28](https://github.com/PrefectHQ/prefect-kubernetes/pull/28)

### Changed

### Deprecated

### Removed

### Fixed

### Security

## 0.1.0

Released November 21, 2022.
### Added
- `KubernetesCredentials` block for generating authenticated Kubernetes clients - [#19](https://github.com/PrefectHQ/prefect-kubernetes/pull/19)
- Tasks for interacting with `job` resources: `create_namespaced_job`, `delete_namespaced_job`, `list_namespaced_job`, `patch_namespaced_job`, `read_namespaced_job`, `replace_namespaced_job` - [#19](https://github.com/PrefectHQ/prefect-kubernetes/pull/19)
- Tasks for interacting with `pod` resources: `create_namespaced_pod`, `delete_namespaced_pod`, `list_namespaced_pod`, `patch_namespaced_pod`, `read_namespaced_pod`, `read_namespaced_pod_logs`, `replace_namespaced_pod` - [#21](https://github.com/PrefectHQ/prefect-kubernetes/pull/21)

- Tasks for interacting with `service` resources: `create_namespaced_service`, `delete_namespaced_service`, `list_namespaced_service`, `patch_namespaced_service`, `read_namespaced_service`, `replace_namespaced_service` - [#22](https://github.com/PrefectHQ/prefect-kubernetes/pull/22)

- Tasks for interacting with `deployment` resources: `create_namespaced_deployment`, `delete_namespaced_deployment`, `list_namespaced_deployment`, `patch_namespaced_deployment`, `read_namespaced_deployment`, `replace_namespaced_deployment` - [#25](https://github.com/PrefectHQ/prefect-kubernetes/pull/25)

### Changed
- `KubernetesCredentials` block to use a single `get_client` method capable of creating all resource-specific client types - [#21](https://github.com/PrefectHQ/prefect-kubernetes/pull/21)
