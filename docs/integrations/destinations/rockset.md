# Rockset

## Features

| Feature | Support |
| :--- | :---: |
| Full Refresh Sync | ✅ |
| Incremental - Append Sync | ✅ |
| Incremental - Deduped History | ❌ |
| Namespaces | ❌ |


## Troubleshooting


## Configuration

| Parameter | Type | Notes |
| :--- | :---: | :--- |
| api_key | string | rockset api key |
| api_server | string | api URL to rockset, specifying http protocol  |
| workspace | string | workspace under which rockset collections will be added/modified |
## Getting Started \(Airbyte Open-Source / Airbyte Cloud\)

#### Requirements

1. Rockset api key with appropriate read and write credentials

## CHANGELOG

| Version | Date | Pull Request | Subject |
| :--- | :--- | :--- | :--- |
| 0.1.2 | 2022-05-17 | [12820](https://github.com/airbytehq/airbyte/pull/12820) | Improved 'check' operation performance |
| 0.1.1 | 2022-02-14 | [10256](https://github.com/airbytehq/airbyte/pull/10256) | Add `-XX:+ExitOnOutOfMemoryError` JVM option |
| 0.1.0 | 2021-11-15 | [\#8006](https://github.com/airbytehq/airbyte/pull/8006) | Initial release|

