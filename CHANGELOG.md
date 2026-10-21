# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0](https://github.com/orchestra-hq/sao-paolo/compare/1.4.0...1.5.0) (2026-10-21)


### Added

* **dbt v2 support:** state-aware orchestration now runs on dbt v2 (`dbt-oss>=2,<3`) as well as dbt-core 1.10–1.12, tested on BigQuery, Snowflake and Databricks. On v2, `orc`:
  * runs dbt's own source freshness ([#98](https://github.com/orchestra-hq/sao-paolo/issues/98)) ([dbf1f17](https://github.com/orchestra-hq/sao-paolo/commit/dbf1f178867424ddafcfecc6863a029b713baab5))
  * resolves `ls` names to paths through the manifest ([#93](https://github.com/orchestra-hq/sao-paolo/issues/93)) ([28c33e2](https://github.com/orchestra-hq/sao-paolo/commit/28c33e29e8cea54b812906f62e943da71503b8ef))
  * checks relations still exist using dbt's catalog ([#101](https://github.com/orchestra-hq/sao-paolo/issues/101)) ([ed3b249](https://github.com/orchestra-hq/sao-paolo/commit/ed3b249ee523a60361a0275ca12508b00cde43de))
  * keeps model checksums the same as on v1, so upgrading doesn't rebuild every model ([#110](https://github.com/orchestra-hq/sao-paolo/issues/110)) ([2f757c4](https://github.com/orchestra-hq/sao-paolo/commit/2f757c43a890cc7fb95cbf1047ab98c1f531578e))
  * always rebuilds models on view sources that have no `loaded_at_*` ([#118](https://github.com/orchestra-hq/sao-paolo/issues/118)) ([1a985c7](https://github.com/orchestra-hq/sao-paolo/commit/1a985c7cefbd9cff94c7d32266024568a5d668bd), [40fa8ed](https://github.com/orchestra-hq/sao-paolo/commit/40fa8eda68c4ff31a7407e59b2418e381018a24f))
* forward --threads and --packages-install-path to in-process dbt commands ([#130](https://github.com/orchestra-hq/sao-paolo/issues/130)) ([83003ee](https://github.com/orchestra-hq/sao-paolo/commit/83003ee60a41d1a0bddd44c661dd0660984adb20))
* Add repo assets and update README ([#122](https://github.com/orchestra-hq/sao-paolo/issues/122)) ([189ced2](https://github.com/orchestra-hq/sao-paolo/commit/189ced2f18e1fd075f6ee9bb58d10a31cdca995e))


### Fixed

* don't read a stale sources.json after a failed freshness run ([#99](https://github.com/orchestra-hq/sao-paolo/issues/99)) ([f4b75ce](https://github.com/orchestra-hq/sao-paolo/commit/f4b75cedafd51160df362d6bbd9a7054cbcd826c))
* find package files under a custom packages-install-path ([#105](https://github.com/orchestra-hq/sao-paolo/issues/105)) ([95255df](https://github.com/orchestra-hq/sao-paolo/commit/95255dfdee84b9a4f6dac229520c5c4a99975645))
* honour --project-dir and --target-path ([#103](https://github.com/orchestra-hq/sao-paolo/issues/103)) ([f3359a1](https://github.com/orchestra-hq/sao-paolo/commit/f3359a181bae204854c2a9f36bc5c32e4231b486))
* keep local-run state and other accounts' entries ([#111](https://github.com/orchestra-hq/sao-paolo/issues/111)) ([3219df9](https://github.com/orchestra-hq/sao-paolo/commit/3219df9796112b5cf5e8f05cfee2d0ad198ec83d))
* read orc settings from the dbt project's pyproject.toml ([#109](https://github.com/orchestra-hq/sao-paolo/issues/109)) ([285dbd5](https://github.com/orchestra-hq/sao-paolo/commit/285dbd56e8cbddb6cae6cf5279a6a93fdf06d024))
* tag reused seeds in a properties file dbt reads ([#104](https://github.com/orchestra-hq/sao-paolo/issues/104)) ([b9e768a](https://github.com/orchestra-hq/sao-paolo/commit/b9e768ab77a0497396a800b7d6218c40eb190d9f))
* restore README banner and demo gif ([#123](https://github.com/orchestra-hq/sao-paolo/issues/123)) ([a7757e1](https://github.com/orchestra-hq/sao-paolo/commit/a7757e1f358de340af4c6a664a7ee6dcd405632a))

## [1.4.0](https://github.com/orchestra-hq/sao-paolo/compare/1.3.1...1.4.0) (2026-10-07)


### Added

* retry saving state to Orchestra up to 3 times with jittered backoff on 5xx, 429 and network errors, so a transient failure no longer drops the run's state ([#88](https://github.com/orchestra-hq/sao-paolo/issues/88)) ([fbc40e0](https://github.com/orchestra-hq/sao-paolo/commit/fbc40e027e846299b8fcb0a03371f52020971e7b))
* send only the nodes a run updated to the Orchestra state API, instead of re-reading and rewriting the account's whole state; concurrent runs no longer overwrite each other or trigger state API 500s, and a run that built nothing no longer saves ([#87](https://github.com/orchestra-hq/sao-paolo/issues/87)) ([12a3eac](https://github.com/orchestra-hq/sao-paolo/commit/12a3eacb7ec58b9ed1045661030690c816189891))


### Fixed

* always log the reuse count ([#92](https://github.com/orchestra-hq/sao-paolo/issues/92)) ([63833db](https://github.com/orchestra-hq/sao-paolo/commit/63833dbfedb3fd3f5f0ae90548e1fd7032871652))
* forward profile flags to the source freshness run ([#90](https://github.com/orchestra-hq/sao-paolo/issues/90)) ([484f72b](https://github.com/orchestra-hq/sao-paolo/commit/484f72b1ce09a7b50e9f2d5002d1f8f09edcb6a0))
* release duckdb file lock before running dbt; add dbt 2.x CI via duckdb ([#89](https://github.com/orchestra-hq/sao-paolo/issues/89)) ([603cf5b](https://github.com/orchestra-hq/sao-paolo/commit/603cf5bfefb5202f8d14ade9ebedf786e8731827))
* strip build-only flags before calling dbt ls ([#91](https://github.com/orchestra-hq/sao-paolo/issues/91)) ([e43fab4](https://github.com/orchestra-hq/sao-paolo/commit/e43fab45bd4b1b816d215ac94ba84e4a0536cee4))
* the supported dbt-core version message quoted &lt;1.12, wrongly warning users on dbt-core 1.12; it now matches the allowed range (&lt;1.13) ([#84](https://github.com/orchestra-hq/sao-paolo/issues/84)) ([abe9fdc](https://github.com/orchestra-hq/sao-paolo/commit/abe9fdcfea7d9047e8af25c0936412817b25665e))

## [1.3.1] - 2026-09-17

### Added

- The warehouse existence check (`verify_relations_exist` / `ORCHESTRA_VERIFY_RELATIONS_EXIST`) now logs how long it took, e.g. `Warehouse existence check for 3 node(s) took 0.42s.`

### Fixed

- `scope_source_freshness_to_selection` collected **zero** sources in projects whose models live in installed packages, silently disabling reuse for every model downstream of a source. The selection was built as `--select +path:<file>`, and dbt resolves `path:` by globbing the real filesystem from the project root — but `dbt ls` reports each node's path relative to the package that owns it, so a package-owned model never matched. dbt treats an empty selection as "Nothing to do": a warning (suppressed by the `-q` we pass) plus a valid, empty `sources.json`, so it surfaced only as `Collected 0 source(s) information.` with no error. Selection is now by dotted fqn (`--select +<fqn>`), which comes from the manifest and is package-qualified.

[1.3.1]: https://github.com/orchestra-hq/sao-paolo/releases/tag/1.3.1

## [1.3.0] - 2026-09-10

### Added

- Verify a node's relation still exists in the target warehouse before reusing it. A model whose table or view was dropped out of band — or a state file pointed at a fresh database or schema — no longer gets silently skipped; it is forced back into the run (`<node> was deleted from the warehouse hence rerun.`) and its downstream models rebuild with it. Delegated to the dbt adapter's own relation listing, so it works on every warehouse dbt supports. Usually costs no extra queries at all — the preceding `dbt source freshness` run already lists every schema and this reads that cache, falling back to one listing per distinct `(database, schema)` when cold. Nothing scales with model count, and there are no queries when nothing is reusable. Off by default while it beds in: opt in with `verify_relations_exist` / `ORCHESTRA_VERIFY_RELATIONS_EXIST`, and existing runs are unaffected until you do. `spark` is excluded.
- `scope_source_freshness_to_selection` setting (`[tool.orchestra_dbt]` or `ORCHESTRA_SCOPE_SOURCE_FRESHNESS_TO_SELECTION`). When enabled, `dbt source freshness` only checks sources upstream of the selection already resolved for node reuse, instead of every source in the project. Each already-resolved path is forwarded as `--select +path:<file>`, so dbt's own selection engine resolves the ancestor sources — no manifest parsing or graph walking on our side. Off by default, so freshness checking behaviour is unchanged until you opt in.

[1.3.0]: https://github.com/orchestra-hq/sao-paolo/releases/tag/1.3.0

## [1.2.0] - 2026-08-27

### Added

- `require_explicit_source_freshness` setting (`[tool.orchestra_dbt]` or `ORCHESTRA_REQUIRE_EXPLICIT_SOURCE_FRESHNESS`). When enabled, sources without an explicit `loaded_at_field`/`loaded_at_query` are excluded from state-aware orchestration — no implicit/fallback freshness is inferred for them and models depending on them always run. Useful when implicit freshness is unreliable, e.g. sources defined on views, where warehouse metadata reflects the view rather than the underlying data.

### Fixed

- `--full-refresh` detection (the switch that disables stateful orchestration for a run) now also recognizes the short flag `-f` and the `DBT_FULL_REFRESH` env var, not just a bare `--full-refresh` token. Previously `orc dbt build -f` or `DBT_FULL_REFRESH=true` triggered a real full refresh in the dbt subprocess while orchestra's own state-aware reuse logic ran as if it hadn't. Env var truthiness matches click's exact recognized states (`1/yes/true/on/t/y` vs `0/no/false/off/f/n/""`), and an explicit flag still wins over the environment.
- `--target` is now resolved from `--target=<name>`, `-t <name>`, `-tname` and `DBT_TARGET`, not just a bare `--target <name>`. Previously those forms silently fell back to the default target, so anything relying on `find_target_in_args` (currently `dbt source freshness`) could inspect a different warehouse than the one dbt actually built into. Note `-t=<name>` deliberately resolves to the literal target `=<name>`, matching a real quirk of dbt's own `click`-based parsing (short options don't split on `=`) rather than "fixing" it into a disagreement with dbt. Resolved values are also no longer stripped of whitespace, matching click: only a truly empty `DBT_TARGET` is treated as unset.

[1.2.0]: https://github.com/orchestra-hq/sao-paolo/releases/tag/1.2.0

## [1.1.1] - 2026-07-06

### Fixed

- Stop clobbering stored state when it cannot be loaded at save time. When merging a run's updates, `save_state` now re-reads the latest state and fails loudly (`StateSaveError`) if that read fails, instead of assuming empty state and overwriting every node the run did not touch.
- The Orchestra HTTP state backend now raises on a failed load (network error, non-2xx, or unparseable body) rather than silently returning empty state, so a transient outage can no longer wipe stored state on the next save.
- A failed state load at the start of a run no longer aborts the dbt command. The run continues with empty state (no node reuse) and still persists state on completion if the backend can be re-read. When the initial load failed, a subsequent save failure is logged as a warning rather than failing the run — a save failure is only fatal when good state was loaded to begin with.

[1.1.1]: https://github.com/orchestra-hq/sao-paolo/releases/tag/1.1.1

## [1.1.0] - 2026-06-30

### Added

- GCS state backend (`ORCHESTRA_STATE_FILE=gs://…`) using Application Default Credentials; install with `dbt-orchestra[gcs]`.
- Azure Blob Storage state backend (`ORCHESTRA_STATE_FILE=abfss://…`); install with `dbt-orchestra[azure]`.

### Fixed

- Keep data tests that span reused and freshly-built models. The reused-node exclusion now uses `cautious` indirect selection, so a test is dropped only when _all_ its parents are reused — matching plain `dbt build`. Applies to bare, `--selector`, and `--select`/`--exclude` commands.
- Restore `selectors.yml` to its pre-run state after a local run, so `--selector` rewrites and generated selectors no longer mutate or accumulate on disk.
- Stop `dbt source freshness` from crashing (`'NoneType' object has no attribute 'filter'`) on sources configured with `config: {freshness: null}`. Such sources now still get a real, queried `max_loaded_at` and always report a passing freshness status, instead of raising or reporting a fabricated timestamp.

[1.1.0]: https://github.com/orchestra-hq/sao-paolo/releases/tag/1.1.0

## [1.0.2] - 2026-06-12

### Changed

- Open sourced under the Apache License 2.0.

[1.0.2]: https://github.com/orchestra-hq/sao-paolo/releases/tag/1.0.2

## [1.0.1] - 2026-05-14

### Fixed

- Skip unsupported node types (`function.*`) in DAG edge construction to prevent `KeyError` when dbt Core includes them in `depends_on.nodes`.

[1.0.1]: https://github.com/orchestra-hq/sao-paolo/releases/tag/1.0.1

## [1.0.0] - 2026-04-24

First formal release of this codebase.

[1.0.0]: https://github.com/orchestra-hq/sao-paolo/releases/tag/1.0.0
