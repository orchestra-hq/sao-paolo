# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-06-30

### Added

- GCS state backend (`ORCHESTRA_STATE_FILE=gs://…`) using Application Default Credentials; install with `dbt-orchestra[gcs]`.
- Azure Blob Storage state backend (`ORCHESTRA_STATE_FILE=abfss://…`); install with `dbt-orchestra[azure]`.

### Fixed

- Keep data tests that span reused and freshly-built models. The reused-node exclusion now uses `cautious` indirect selection, so a test is dropped only when _all_ its parents are reused — matching plain `dbt build`. Applies to bare, `--selector`, and `--select`/`--exclude` commands.
- Restore `selectors.yml` to its pre-run state after a local run, so `--selector` rewrites and generated selectors no longer mutate or accumulate on disk.

[1.1.0]: https://github.com/orchestra-hq/sao-paolo/releases/tag/v1.1.0

## [1.0.2] - 2026-06-12

### Changed

- Open sourced under the Apache License 2.0.

[1.0.2]: https://github.com/orchestra-hq/sao-paolo/releases/tag/v1.0.2

## [1.0.1] - 2026-05-14

### Fixed

- Skip unsupported node types (`function.*`) in DAG edge construction to prevent `KeyError` when dbt Core includes them in `depends_on.nodes`.

[1.0.1]: https://github.com/orchestra-hq/sao-paolo/releases/tag/v1.0.1

## [1.0.0] - 2026-04-24

First formal release of this codebase.

[1.0.0]: https://github.com/orchestra-hq/sao-paolo/releases/tag/v1.0.0
