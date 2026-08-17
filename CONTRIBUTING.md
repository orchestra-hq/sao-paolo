# Contributing

We welcome community contributions that improve this project's capabilities in helping teams optimise their pipeline runs with confidence. To keep that impact broad and reliable, we will avoid introducing breaking changes wherever reasonable.

## Contributor workflow

1. Create a branch
1. Make focused code changes, adding or updating unit tests when behaviour changes
1. Ensure linting and unit testing passes, using `uv run ruff check .`, `uv run basedpyright` and `uv run pytest`
1. Where possible, test behaviour locally
1. Test in Orchestra with the branch
1. Raise a pull request against the `main` branch, describing what changed and why

Pull requests run GitHub Actions: unit tests, static checks, `dbt build` for `tutorial/dbt` against Postgres, and an Orchestra pipeline via the [Orchestra Run Pipeline Action](https://github.com/orchestra-hq/run-pipeline).

## Debugging

To debug pipelines, there are some local files and scripts.

To install the required dependencies, run:

```bash
uv sync --extra dev --extra debug
```

Ask @ojc-orchestra for access to the scripts:

- `dynamo_state.py`: loads state from DynamoDB into a local JSON file `local_state.json`
- `visualise.py`: loads ops from `ops.json` and visualises them in a DAG structure. This can be loaded in a browser by opening the resulting HTML file, `ops_dag.html`.

## Testing

```bash
pytest
```

Without Postgres, the tutorial `dbt build` integration test is skipped. To run it locally, start Postgres, set `PGHOST`, `PGDATABASE`, and related variables (see [`tutorial/README.md`](tutorial/README.md)), then run `pytest tests/integration/test_tutorial_dbt.py`.

For the optional DAG integration test (`tests/integration/test_local.py`), place both `local_state.json` and `local_manifest.json` in the repository root. `local_state.json` can be created with `dynamo_state.py` (see above), and `local_manifest.json` can be downloaded from a representative dbt run.

Run only unit or integration tests:

```bash
pytest tests/unit/
pytest tests/integration/
```

Run a specific test module or case:

```bash
pytest tests/unit/test_state.py
pytest tests/unit/test_state.py::TestLoadState::test_load_state_success
```

## Linting

```bash
ruff check . && ruff format --check . && basedpyright
```

To automatically fix issues:

```bash
ruff check --fix . && ruff format . && basedpyright
```

## Source freshness: adding support for a new dbt adapter

Orchestra patches dbt’s freshness runner so that when a source has **no** `loaded_at_query` or `loaded_at_field`, adapter-specific logic can still infer `max_loaded_at` (for example Databricks uses `DESCRIBE HISTORY`). That path is keyed by the dbt adapter type string from `FreshnessRunner.adapter.type()` (for example `"databricks"`).

End-user expectations by warehouse are summarized in the root **`README.md`** (section *Warehouse adapters and implicit source freshness*): Databricks has the `DESCRIBE HISTORY` fallback; Snowflake, Fabric, and Postgres rely on standard dbt freshness with `loaded_at_*` configured; DuckDB does not support this current form of state aware orchestration.

To add a warehouse:

1. Implement a handler in `src/orchestra_dbt/source_freshness/fallbacks/`, for example `try_snowflake_fallback(runner, compiled_node, manifest)`, that returns a dbt `SourceFreshnessResult` or `None` if the fallback does not apply or fails.
2. Use `runner` (the freshness runner) to run SQL via `runner.adapter` the same way the existing Databricks module does. Helpers in `source_freshness/fallbacks/common.py` (`parse_query_timestamp_cell`, `build_source_freshness_result_from_loaded_at`) match the result shape dbt expects once you have a timezone-aware `max_loaded_at`.
3. Register the handler in `source_freshness/fallbacks/registry.py`: add an import and map the exact adapter type string to your function in `FALLBACK_BY_ADAPTER_TYPE`. Unknown adapters are ignored; dbt’s normal path runs when both loaded-at fields are set.
4. Add or extend unit tests under `tests/unit/` (see `test_source_freshness_fallbacks.py`). Code that imports dbt artifact types can use `pytest.importorskip("dbt.artifacts")` so the suite still runs when only minimal dev extras are installed.

## Relation existence checks: adjusting an adapter

Before reusing a node, `src/orchestra_dbt/warehouse/` confirms its relation is actually in the target warehouse and forces missing nodes back into the run (see *Verifying relations still exist* in the root **`README.md`**). Two things make this cheap and adapter-agnostic, and both are worth preserving:

- It reuses the dbt adapter **already registered in this process** by the in-process `dbt source freshness` invocation (`acquire_in_process_adapter` in `warehouse/adapter_session.py`). There is no second profile bootstrap and no extra project parse.
- It issues **one listing per `(database, schema)`**, never one call per model, and compares identifiers in memory. Note that `adapter.get_relation()` is deliberately *not* used: `BaseRelation.matches` raises `ApproximateMatchError` on a case-only mismatch instead of reporting the relation absent.

To change the behaviour for one warehouse, edit `src/orchestra_dbt/warehouse/registry.py`:

1. **To exclude an adapter**, add its exact adapter type string to `EXISTENCE_CHECK_UNSUPPORTED`. Do this when the adapter's listing is expensive (it must stay one call per schema) or when it swallows errors and returns an empty list, which would read as "the whole schema is gone" and rebuild everything. `spark` is excluded for both reasons.
2. **To replace the listing**, add a handler to `EXISTENCE_OVERRIDE_BY_ADAPTER_TYPE` taking `(adapter, schema_relation)` and returning the set of identifiers in that schema, or `None` to fall through to dbt's own `list_relations_without_caching`. Run SQL via `adapter.execute(...)` the same way `source_freshness/fallbacks/databricks.py` does; identifiers are normalised for you.
3. Keep the failure posture: a schema that cannot be read must yield `RelationExistence.UNKNOWN` for its nodes only, leaving their reuse decisions untouched. Never let a metadata failure become a full rebuild.
4. Update the per-warehouse cost table in `README.md` and add tests under `tests/unit/` (see `test_warehouse_existence.py`, which uses a `MagicMock` adapter and asserts the one-call-per-schema budget).

## State storage: adding a new file-backed backend

Stateful orchestration loads and saves a `StateApiModel` JSON document. Backends implement the `StateBackend` protocol in `src/orchestra_dbt/state_backends/base.py`: `load() -> StateApiModel` and `save(state: StateApiModel) -> None`.

Today, non-HTTP persistence is selected from `state_file` / `ORCHESTRA_STATE_FILE` as either a **local path** or an **`s3://bucket/key`** URI (see `README.md` for precedence with `ORCHESTRA_API_KEY`). To add another destination (for example another object store or URI scheme):

1. Add a `StateBackendKind` value and any fields needed on `StateBackendConfig` in `src/orchestra_dbt/state_types.py`. Extend `backend_config_from_state_location()` (and, if needed, `resolve_state_backend_config()` in `state_backends/factory.py`) so the new URI or path pattern resolves to that config.
2. Add a module under `src/orchestra_dbt/state_backends/` with a class that implements `load` / `save`. Follow existing backends: validate JSON, use `StateApiModel.model_validate`, call `apply_integration_account_filter(state)` after load, raise `StateLoadError` / `StateSaveError` from `state_errors.py` on failure, and use `state_backends/logging.py` for consistent log lines (extend `StateBackendLabel` and the label maps there when you add a new backend name).
3. Wire the backend in `resolved_state_backend()` in `state_backends/factory.py` (`match` on `StateBackendKind`). Prefer **lazy imports** inside the branch if the backend needs optional third-party packages, and declare those under `[project.optional-dependencies]` in `pyproject.toml` (the S3 backend uses the `s3` extra and `boto3` this way).
4. Add tests for config parsing and backend behaviour under `tests/unit/` (config tests already cover S3 URI parsing in `test_config.py`).

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
