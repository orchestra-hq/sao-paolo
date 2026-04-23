# Contributing

## Pull requests

Target `main`. Keep changes focused; add or update tests when behaviour changes. Describe what changed and why.

Match local checks to CI where practical: `uv run ruff check .`, `uv run basedpyright`, and `uv run pytest`. Optional debug tooling is documented in `README.md` (`--extra debug`).

## Source freshness: adding support for a new dbt adapter

Orchestra patches dbt’s freshness runner so that when a source has **no** `loaded_at_query` or `loaded_at_field`, adapter-specific logic can still infer `max_loaded_at` (for example Databricks uses `DESCRIBE HISTORY`). That path is keyed by the dbt adapter type string from `FreshnessRunner.adapter.type()` (for example `"databricks"`).

End-user expectations by warehouse are summarized in the root **`README.md`** (section *Warehouse adapters and implicit source freshness*): Databricks has the `DESCRIBE HISTORY` fallback; Snowflake, Fabric, and Postgres rely on standard dbt freshness with `loaded_at_*` configured; DuckDB does not support implicit freshness without those fields.

To add a warehouse:

1. Implement a handler in `src/orchestra_dbt/source_freshness/fallbacks/`, for example `try_snowflake_fallback(runner, compiled_node, manifest)`, that returns a dbt `SourceFreshnessResult` or `None` if the fallback does not apply or fails.
2. Use `runner` (the freshness runner) to run SQL via `runner.adapter` the same way the existing Databricks module does. Helpers in `source_freshness/fallbacks/common.py` (`parse_query_timestamp_cell`, `build_source_freshness_result_from_loaded_at`) match the result shape dbt expects once you have a timezone-aware `max_loaded_at`.
3. Register the handler in `source_freshness/fallbacks/registry.py`: add an import and map the exact adapter type string to your function in `FALLBACK_BY_ADAPTER_TYPE`. Unknown adapters are ignored; dbt’s normal path runs when both loaded-at fields are set.
4. Add or extend unit tests under `tests/unit/` (see `test_source_freshness_fallbacks.py`). Code that imports dbt artifact types can use `pytest.importorskip("dbt.artifacts")` so the suite still runs when only minimal dev extras are installed.

## State storage: adding a new file-backed backend

Stateful orchestration loads and saves a `StateApiModel` JSON document. Backends implement the `StateBackend` protocol in `src/orchestra_dbt/state_backends/base.py`: `load() -> StateApiModel` and `save(state: StateApiModel) -> None`.

Today, non-HTTP persistence is selected from `state_file` / `ORCHESTRA_STATE_FILE` as either a **local path** or an **`s3://bucket/key`** URI (see `README.md` for precedence with `ORCHESTRA_API_KEY`). To add another destination (for example another object store or URI scheme):

1. Add a `StateBackendKind` value and any fields needed on `StateBackendConfig` in `src/orchestra_dbt/state_types.py`. Extend `backend_config_from_state_location()` (and, if needed, `resolve_state_backend_config()` in `state_backends/factory.py`) so the new URI or path pattern resolves to that config.
2. Add a module under `src/orchestra_dbt/state_backends/` with a class that implements `load` / `save`. Follow existing backends: validate JSON, use `StateApiModel.model_validate`, call `apply_integration_account_filter(state)` after load, raise `StateLoadError` / `StateSaveError` from `state_errors.py` on failure, and use `state_backends/logging.py` for consistent log lines (extend `StateBackendLabel` and the label maps there when you add a new backend name).
3. Wire the backend in `resolved_state_backend()` in `state_backends/factory.py` (`match` on `StateBackendKind`). Prefer **lazy imports** inside the branch if the backend needs optional third-party packages, and declare those under `[project.optional-dependencies]` in `pyproject.toml` (the S3 backend uses the `s3` extra and `boto3` this way).
4. Add tests for config parsing and backend behaviour under `tests/unit/` (config tests already cover S3 URI parsing in `test_config.py`).

## License

This project is under the [Elastic License 2.0](LICENSE).
