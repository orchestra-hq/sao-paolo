# sao-paolo

## Compatibility

- **Python:** 3.11 through 3.14 (see `requires-python` in `pyproject.toml`). **3.14:** installs and the non-dbt test suite runs in CI; **dbt-core 1.11** still fails to import on CPython 3.14 in current stacks (mashumaro), so **stateful orchestration** (`ORCHESTRA_USE_STATEFUL=true`) and dbt import smoke tests are only meaningful on **3.11–3.13** until upstream fixes that. Use 3.11–3.13 for real dbt runs.
- **dbt-core:** 1.10.x and 1.11.x when using stateful orchestration. The CLI checks the installed `dbt-core` version before invoking `dbt ls` / source freshness. Warehouse adapters are optional: `uv sync --extra dev --extra adapters` (Snowflake/Databricks) when you need them locally.

Integration smoke tests in `tests/integration/test_dbt_smoke.py` import the same dbt internal modules used for `dbt ls` and source freshness (skipped on 3.14 until dbt imports cleanly there).

## Installing

```bash
python3 -m venv .venv
source .venv/bin/activate
uv sync --extra dev
# Optional: Snowflake/Databricks adapters for local runs (adds heavy deps; may not install on bleeding-edge Python yet)
# uv sync --extra dev --extra adapters
```

## Development

1. Create a branch
1. Add the code and unit tests
1. Where possible, [test locally](#running-locally)
1. Test in Orchestra [with the branch](#running-in-orchestra)
1. Raise a PR

## Running locally

```bash
ORCHESTRA_ENV=dev ORCHESTRA_API_KEY=<API_KEY> ORCHESTRA_USE_STATEFUL=true ORCHESTRA_LOCAL_RUN=true orchestra-dbt dbt run --target snowflake
```

## Running in Orchestra

The branch of this project that is run in Orchestra can be set by the environment variable on the task run:

```json
{
    "ORCHESTRA_DBT_BRANCH": "main/feature/whatever"
}
```

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

To run an integration test, you will require both `local_state.json` and `local_manifest.json` in the root directory. `local_state.json` can be created by running `dynamo_state.py` (see above), and `local_manifest.json` can be created by downloading a relevant dbt manifest.json file.

## Linting

```bash
ruff check . && ruff format --check . && basedpyright
```

To automatically fix issues:

```bash
ruff check --fix . && ruff format . && basedpyright
```
