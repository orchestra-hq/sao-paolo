# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Stop dropping data tests that span both reused and freshly-built models. Reused
  nodes are excluded via `--exclude tag:ORCHESTRA_REUSED_NODE`, and dbt's default
  "eager" indirect selection drops a test if _any_ of its parents is excluded, so
  a singular test referencing a reused model alongside a built one was silently
  skipped. The reused-node exclusion now uses `cautious` indirect selection, which
  only drops a test when _all_ of its parents are reused — restoring default
  `dbt build` behaviour where a test runs if any of its models is built. This holds
  whether the command is bare (`dbt build`), uses a named `--selector`, or supplies
  its own `--select`/`--exclude`: in the last case the user's selection and the
  cautious reused-node exclusion are folded into a generated selector (written to
  `selectors.yml`) so only the exclusion is cautious and the user's own selection
  keeps dbt's default eager behaviour.

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
