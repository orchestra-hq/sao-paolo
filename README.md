# sao-paolo

## Compatibility

- **Python:** 3.11, 3.12, and 3.13 only (see `requires-python` in `pyproject.toml`).
- **dbt-core:** 1.10.x and 1.11.x when using stateful orchestration. The CLI checks the installed `dbt-core` version before invoking `dbt ls` / source freshness. Warehouse adapters are optional: `uv sync --extra dev --extra adapters` (Snowflake/Databricks) when you need them locally.

## Installing

```bash
python3 -m venv .venv
source .venv/bin/activate
uv sync --extra dev
# Optional: Snowflake/Databricks adapters for local runs
# uv sync --extra dev --extra adapters
```

## Development

1. Create a branch
1. Add the code and unit tests
1. Where possible, [test locally](#running-locally)
1. Test in Orchestra [with the branch](#running-in-orchestra)
1. Raise a PR

## Stateful mode and where state is stored

When `ORCHESTRA_USE_STATEFUL=true`, the CLI must load and save [dbt Core state](https://docs.getdbt.com/) metadata used for orchestration. That state is the same JSON shape whether it comes from Orchestra’s HTTP API or from a local file.

**Do not put secrets in `pyproject.toml`.** Use environment variables (or your platform’s secret store) for `ORCHESTRA_API_KEY`.

### Choosing HTTP (Orchestra cloud) vs a local JSON file

The CLI discovers `pyproject.toml` by walking upward from the current working directory. `[tool.orchestra_dbt]` is read from that file when present.

| Priority | Setting | Effect |
| --- | --- | --- |
| 1 | `ORCHESTRA_STATE_FILE` | Path to a JSON file. Relative paths are resolved from the current working directory. |
| 2 | `[tool.orchestra_dbt]` / `state_file` in `pyproject.toml` | Path to a JSON file. Relative paths are resolved from the directory that contains the **discovered** `pyproject.toml`; absolute paths are used as-is. |
| 3 | Neither of the above, but `ORCHESTRA_API_KEY` is set | Load/save state via Orchestra HTTP. `ORCHESTRA_ENV` must be one of `app`, `stage`, or `dev` (it defaults to `app` if unset). |

If an effective file path is configured (rows 1 or 2), that **file backend** is used and an API key is not required for state. If only `ORCHESTRA_API_KEY` is set (and no file path), the **HTTP backend** is used.

Stateful orchestration only runs for `dbt build`, `dbt run`, and `dbt test`. Other dbt subcommands are passed through to dbt unchanged.

Example optional snippet in `pyproject.toml`:

```toml
[tool.orchestra_dbt]
state_file = ".orchestra/dbt_state.json"
```

Add `.orchestra/` (or your chosen path) to `.gitignore` if the file should not be committed.

### Bootstrapping a new local state file

If the configured file does not exist, the CLI fails with an error (it does not silently start from empty state). Create a minimal file first, for example:

```bash
mkdir -p .orchestra
echo '{"state":{}}' > .orchestra/dbt_state.json
```

## Running locally

Orchestra HTTP (requires an API key from Orchestra). Do not set `ORCHESTRA_STATE_FILE` or `state_file` in `pyproject.toml` if you want the HTTP backend rather than a local file.

```bash
ORCHESTRA_ENV=dev ORCHESTRA_API_KEY=<API_KEY> ORCHESTRA_USE_STATEFUL=true ORCHESTRA_LOCAL_RUN=true orc run --target snowflake
```

Local JSON file (after creating the file as above), no Orchestra API key required for state:

```bash
ORCHESTRA_USE_STATEFUL=true ORCHESTRA_STATE_FILE=.orchestra/dbt_state.json ORCHESTRA_LOCAL_RUN=true orc run --target snowflake
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

To run the integration test in `tests/integration/test_local.py`, you will require both `local_state.json` and `local_manifest.json` in the root directory. `local_state.json` can be created by running `dynamo_state.py` (see above), and `local_manifest.json` can be created by downloading a relevant dbt `manifest.json` file.

## Linting

```bash
ruff check . && ruff format --check . && basedpyright
```

To automatically fix issues:

```bash
ruff check --fix . && ruff format . && basedpyright
```
