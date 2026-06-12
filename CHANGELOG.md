# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
