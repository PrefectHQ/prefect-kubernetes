# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- `KubernetesCredentials` block for generating authenticated Kubernetes clients - [#19](https://github.com/PrefectHQ/prefect-kubernetes/pull/19)
- Tasks for interacting with `job` resources: `create_namespaced_job`, `delete_namespaced_job`, `list_namespaced_job`, `patch_namespaced_job`, `read_namespaced_job`, `replace_namespaced_job` - [#19](https://github.com/PrefectHQ/prefect-kubernetes/pull/19)
- Tasks for interacting with `pod` resources: `create_namespaced_pod`, `delete_namespaced_pod`, `list_namespaced_pod`, `patch_namespaced_pod`, `read_namespaced_pod`, `read_namespaced_pod_logs`, `replace_namespaced_pod` - [#21](https://github.com/PrefectHQ/prefect-kubernetes/pull/21)
- Tasks for interacting with `service` resources: `create_namespaced_service`, `delete_namespaced_service`, `list_namespaced_service`, `patch_namespaced_service`, `read_namespaced_service`, `replace_namespaced_service` - [#23](https://github.com/PrefectHQ/prefect-kubernetes/pull/23)

### Changed
- `KubernetesCredentials` block to use a single `get_client` method capable of creating all resource-specific client types - [#21](https://github.com/PrefectHQ/prefect-kubernetes/pull/21)


### Deprecated

### Removed

### Fixed

### Security
