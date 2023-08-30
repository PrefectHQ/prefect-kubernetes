# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

- Log more information when a Pod cannot be scheduled - [#87](https://github.com/PrefectHQ/prefect-kubernetes/issues/87)

- Log more information when a Pod cannot start - [#90](https://github.com/PrefectHQ/prefect-kubernetes/issues/90)


### Added

- Handling for spot instance eviction - [#85](https://github.com/PrefectHQ/prefect-kubernetes/pull/85)

### Changed

### Deprecated

### Removed

### Fixed

### Security

## 0.2.11

Released July 20th, 2023.

### Changed
- Promoted workers to GA, removed beta disclaimers

## 0.2.10

Released June 30th, 2023.

### Added

- Handling for rescheduled Kubernetes jobs - [#78](https://github.com/PrefectHQ/prefect-kubernetes/pull/78)

## 0.2.9

Released June 20th, 2023.

### Fixed

- Fixed issue where `generateName` was not populating correctly for some flow runs submitted by `KubernetesWorker` - [#76](https://github.com/PrefectHQ/prefect-kubernetes/pull/76)

## 0.2.8

Released May 25th, 2023.

### Changed

- Improve failure message when creating a Kubernetes job fails - [#71](https://github.com/PrefectHQ/prefect-kubernetes/pull/71)
- Stream Kubernetes Worker flow run logs to the API - [#72](https://github.com/PrefectHQ/prefect-kubernetes/pull/72)

## 0.2.7

Released May 4th, 2023.

### Added

- Fixed issue where `KubernetesEventReplicator` would not shutdown after a flow-run was completed resulting in new flow-runs not being picked up and the worker hanging on exit. - [#57](https://github.com/PrefectHQ/prefect-kubernetes/pull/57)
## 0.2.6

Released April 28th, 2023.

### Added

- `KubernetesEventReplicator` which replicates kubernetes pod events to Prefect events.

## 0.2.5

Released April 20th, 2023.

### Added

- `kill_infrastructure` method on `KubernetesWorker` which stops jobs for cancelled flow runs  - [#52](https://github.com/PrefectHQ/prefect-kubernetes/pull/52)

## 0.2.4

Released April 6th, 2023.

###

- Temporary `prefect` version guard - [#48](https://github.com/PrefectHQ/prefect-kubernetes/pull/48)
- Advanced configuration documentation - [#50](https://github.com/PrefectHQ/prefect-kubernetes/pull/50)

## 0.2.3

Released April 1, 2023.

### Added

- Custom objects crud tasks for kubernetes custom resource definitions. - [#45](https://github.com/PrefectHQ/prefect-kubernetes/pull/45)

### Changed

- Refactor KubernetesJob and KubernetesJobRun to use existing task functions - [#43](https://github.com/PrefectHQ/prefect-kubernetes/pull/43)

## 0.2.2

Released March 24, 2023.

### Added

- Experimental `KubernetesWorker` for executing flow runs within Kubernetes jobs - [#40](https://github.com/PrefectHQ/prefect-kubernetes/pull/40)

## 0.2.1

Released February 17, 2023.

### Added
- Sync compatibility for block method calls used by `run_namespaced_job` - [#34](https://github.com/PrefectHQ/prefect-kubernetes/pull/34)

## 0.2.0

Released December 23, 2022.

### Added
- `KubernetesJob` block for running a Kubernetes job from a manifest - [#28](https://github.com/PrefectHQ/prefect-kubernetes/pull/28)
- `run_namespaced_job` flow allowing easy execution of a well-specified `KubernetesJob` block on a cluster specified by `KubernetesCredentials` - [#28](https://github.com/PrefectHQ/prefect-kubernetes/pull/28)
- `convert_manifest_to_model` utility function for converting a Kubernetes manifest to a model object - [#28](https://github.com/PrefectHQ/prefect-kubernetes/pull/28)

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
