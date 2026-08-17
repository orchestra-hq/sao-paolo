# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-08-27

### Added

- `require_explicit_source_freshness` setting (`[tool.orchestra_dbt]` or `ORCHESTRA_REQUIRE_EXPLICIT_SOURCE_FRESHNESS`). When enabled, sources without an explicit `loaded_at_field`/`loaded_at_query` are excluded from state-aware orchestration — no implicit/fallback freshness is inferred for them and models depending on them always run. Useful when implicit freshness is unreliable, e.g. sources defined on views, where warehouse metadata reflects the view rather than the underlying data.

### Added

- Verify a node's relation still exists in the target warehouse before reusing it. A model whose table or view was dropped out of band — or a state file pointed at a fresh database or schema — no longer gets silently skipped; it is forced back into the run (`<node> was deleted from the warehouse hence rerun.`) and its downstream models rebuild with it. Delegated to the dbt adapter's own relation listing, so it works on every warehouse dbt supports, and costs one metadata query per distinct `(database, schema)` holding a reusable node — nothing scales with model count, and there are no queries when nothing is reusable. Controlled by `verify_relations_exist` / `ORCHESTRA_VERIFY_RELATIONS_EXIST` (default on); `spark` is excluded.

### Fixed

- `--full-refresh` detection (the switch that disables stateful orchestration for a run) now also recognizes the short flag `-f` and the `DBT_FULL_REFRESH` env var, not just a bare `--full-refresh` token. Previously `orc dbt build -f` or `DBT_FULL_REFRESH=true` triggered a real full refresh in the dbt subprocess while orchestra's own state-aware reuse logic ran as if it hadn't. Env var truthiness matches click's exact recognized states (`1/yes/true/on/t/y` vs `0/no/false/off/f/n/""`), and an explicit flag still wins over the environment.
- `--target` is now resolved from `--target=<name>`, `-t <name>`, `-tname` and `DBT_TARGET`, not just a bare `--target <name>`. Previously those forms silently fell back to the default target, so anything relying on `find_target_in_args` (currently `dbt source freshness`) could inspect a different warehouse than the one dbt actually built into. Note `-t=<name>` deliberately resolves to the literal target `=<name>`, matching a real quirk of dbt's own `click`-based parsing (short options don't split on `=`) rather than "fixing" it into a disagreement with dbt. Resolved values are also no longer stripped of whitespace, matching click: only a truly empty `DBT_TARGET` is treated as unset.

[1.2.0]: https://github.com/orchestra-hq/sao-paolo/releases/tag/v1.2.0

## [1.1.1] - 2026-07-06

### Fixed

- Stop clobbering stored state when it cannot be loaded at save time. When merging a run's updates, `save_state` now re-reads the latest state and fails loudly (`StateSaveError`) if that read fails, instead of assuming empty state and overwriting every node the run did not touch.
- The Orchestra HTTP state backend now raises on a failed load (network error, non-2xx, or unparseable body) rather than silently returning empty state, so a transient outage can no longer wipe stored state on the next save.
- A failed state load at the start of a run no longer aborts the dbt command. The run continues with empty state (no node reuse) and still persists state on completion if the backend can be re-read. When the initial load failed, a subsequent save failure is logged as a warning rather than failing the run — a save failure is only fatal when good state was loaded to begin with.

[1.1.1]: https://github.com/orchestra-hq/sao-paolo/releases/tag/v1.1.1

## [1.1.0] - 2026-06-30

### Added

- GCS state backend (`ORCHESTRA_STATE_FILE=gs://…`) using Application Default Credentials; install with `dbt-orchestra[gcs]`.
- Azure Blob Storage state backend (`ORCHESTRA_STATE_FILE=abfss://…`); install with `dbt-orchestra[azure]`.

### Fixed

- Keep data tests that span reused and freshly-built models. The reused-node exclusion now uses `cautious` indirect selection, so a test is dropped only when _all_ its parents are reused — matching plain `dbt build`. Applies to bare, `--selector`, and `--select`/`--exclude` commands.
- Restore `selectors.yml` to its pre-run state after a local run, so `--selector` rewrites and generated selectors no longer mutate or accumulate on disk.
- Stop `dbt source freshness` from crashing (`'NoneType' object has no attribute 'filter'`) on sources configured with `config: {freshness: null}`. Such sources now still get a real, queried `max_loaded_at` and always report a passing freshness status, instead of raising or reporting a fabricated timestamp.

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
