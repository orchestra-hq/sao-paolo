# dbt-orchestra

## Compatibility

- **Python:** 3.11, 3.12, and 3.13 only (see `requires-python` in `pyproject.toml`).
- **dbt-core:** 1.10.x and 1.11.x when using stateful orchestration.

## Installing

```bash
python3 -m venv .venv
source .venv/bin/activate
uv sync --extra dev
# Optional: Snowflake/Databricks adapters for local runs
# uv sync --extra dev --extra adapters
```

## Tutorial dbt project

A minimal dbt Core project lives under `[tutorial/](tutorial/)`. This is used for testing and CI.

## Development

1. Create a branch
1. Add the code and unit tests
1. Where possible, [test locally](#running-locally)
1. Test in Orchestra with the branch
1. Raise a PR

Pull requests run GitHub Actions: unit tests, static checks, `dbt build` for `tutorial/dbt` against Postgres, and an Orchestra pipeline via the [Orchestra Run Pipeline Action](https://github.com/orchestra-hq/run-pipeline).

## State Aware Orchestration

When stateful orchestration is enabled, the CLI loads and saves [dbt Core state](https://docs.getdbt.com/). Enable it with `use_stateful = true` under `[tool.orchestra_dbt]`, or set `ORCHESTRA_USE_STATEFUL=true`. That state is the same JSON shape regardless of the backend used.

**Do not put secrets in `pyproject.toml`.** Use environment variables (or your platform’s secret store) for `ORCHESTRA_API_KEY`.

### Configuration precedence

For non-secret options, **if an environment variable is set, it overrides** values from `[tool.orchestra_dbt]`; otherwise the value from `pyproject.toml` is used, then the built-in default.

### `[tool.orchestra_dbt]` options

| Key                      | Type              | Default | Purpose                                                                   |
| ------------------------ | ----------------- | ------- | ------------------------------------------------------------------------- |
| `state_file`             | string (optional) | —       | Local JSON path or `s3://bucket/key` for state (see backend table below). |
| `use_stateful`           | bool              | `false` | Turn on stateful orchestration for supported dbt commands.                |
| `orchestra_env`          | string            | `app`   | Orchestra deployment: `app`, `stage`, or `dev` (HTTP API host).           |
| `local_run`              | bool              | `true`  | After reuse, revert patched files (typical for local iteration).          |
| `debug`                  | bool              | `false` | Verbose debug logging.                                                    |
| `integration_account_id` | string (optional) | —       | When set, filter state keys to this integration account prefix.           |

Equivalent environment overrides (when set): `ORCHESTRA_USE_STATEFUL`, `ORCHESTRA_ENV`, `ORCHESTRA_LOCAL_RUN`, `ORCHESTRA_DBT_DEBUG`, `ORCHESTRA_INTEGRATION_ACCOUNT_ID`.

### Choosing HTTP (Orchestra cloud), a local JSON file, or S3

The CLI discovers `pyproject.toml` by walking upward from the current working directory. `[tool.orchestra_dbt]` is read from that file when present.

| Priority | Setting                                                   | Effect                                                                                                                                                                                                                                                                                                                   |
| -------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1        | `ORCHESTRA_API_KEY`                                       | Load/save state via Orchestra HTTP. The Orchestra environment is `orchestra_env` in pyproject (default `app`) or `ORCHESTRA_ENV` when set; must be one of `app`, `stage`, or `dev`. When the API key is set, `ORCHESTRA_STATE_FILE` and `state_file` in `pyproject.toml` are **ignored** for choosing the state backend. |
| 2        | `ORCHESTRA_STATE_FILE`                                    | Path to a JSON file, or `s3://bucket/key` for an object in S3. Relative file paths are resolved from the current working directory. Used only when `ORCHESTRA_API_KEY` is unset.                                                                                                                                         |
| 3        | `[tool.orchestra_dbt]` / `state_file` in `pyproject.toml` | Path to a JSON file, or `s3://bucket/key`. Relative file paths are resolved from the directory that contains the **discovered** `pyproject.toml`; absolute paths are used as-is. Used only when `ORCHESTRA_API_KEY` is unset and `ORCHESTRA_STATE_FILE` is unset.                                                        |

If an effective local path or S3 URI is configured (rows 2 or 3), that **file** or **S3** backend is used and an API key is not required for state. If `ORCHESTRA_API_KEY` is set (row 1), the **HTTP backend** is used regardless of file settings.

For S3, install the optional dependency (`pip install 'dbt-orchestra[s3]'` or `uv sync --extra s3`). Credentials and region follow the usual [AWS SDK resolution](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) (environment variables, shared config, IAM role, etc.). If the object does not exist yet, load starts with an empty state and save creates the object.

Stateful orchestration only runs for `dbt build`, `dbt run`, and `dbt test`. Other dbt subcommands are passed through to dbt unchanged.

### Warehouse adapters and implicit source freshness

Stateful reuse uses `dbt source freshness` results. When a source defines `**loaded_at_field`** or `**loaded_at_query**`, dbt’s normal freshness logic runs on every adapter Orchestra supports through dbt Core.

When **both** are omitted, Orchestra can still run **adapter-specific** SQL to infer `max_loaded_at` (see `src/orchestra_dbt/source_freshness/`). Only the adapters below register that path today; the mapping is keyed by `FreshnessRunner.adapter.type()`.

| Warehouse            | dbt adapter type (typical) | Implicit freshness (no `loaded_at_*`)                                                                             |
| -------------------- | -------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| **Databricks**       | `databricks`               | **Supported** — uses `DESCRIBE HISTORY` on the source relation.                                                   |
| **Snowflake**        | `snowflake`                | **Use `loaded_at_field` or `loaded_at_query`** — no Orchestra fallback; standard dbt freshness.                   |
| **Microsoft Fabric** | `fabric`                   | Same as Snowflake — configure `loaded_at_`*; no Orchestra fallback.                                               |
| **PostgreSQL**       | `postgres`                 | Same as Snowflake — configure `loaded_at_`*; no Orchestra fallback.                                               |
| **DuckDB**           | `duckdb`                   | **Not supported**                                                                                                 |
| **Other adapters**   | varies                     | No Orchestra fallback unless listed above; use `loaded_at_`* or verify dbt’s default behavior for your warehouse. |

For adapters without a registered fallback, if both `loaded_at` settings are missing, Orchestra follows dbt’s `FreshnessRunner` behavior (which may surface as warnings or a non-actionable result depending on dbt and the warehouse).

### Runtime behavior by command and mode

| Stateful enabled | dbt command                                         | Behavior                                                                                                            |
| ---------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `false`          | any command                                         | `orc` passes through to dbt with no state load/save.                                                                |
| `true`           | `build`, `run`, `test`                              | `orc` loads state, computes reusable nodes, patches clean nodes, runs dbt, updates and saves state.                 |
| `true`           | `build`, `run`, `test` + `--full-refresh`           | `orc` skips reuse decisions for this invocation, runs dbt directly, then still updates/saves state after execution. |
| `true`           | other command (for example `seed`, `docs generate`) | `orc` passes through to dbt unchanged.                                                                              |

Example optional snippet in `pyproject.toml`:

```toml
[tool.orchestra_dbt]
use_stateful = true
orchestra_env = "dev"
state_file = ".orchestra/dbt_state.json"
```

Add `.orchestra/` (or your chosen path) to `.gitignore` if the file should not be committed.

### Bootstrapping a new local state file

If the configured **local** file does not exist, the CLI fails with an error (it does not silently start from empty state). Create a minimal file first, for example:

```bash
mkdir -p .orchestra
echo '{"state":{}}' > .orchestra/dbt_state.json
```

## Running locally

Orchestra HTTP (requires an API key from Orchestra). Setting `ORCHESTRA_API_KEY` selects the HTTP backend; file-related settings are ignored. Put non-secret defaults in `pyproject.toml` and only export the API key:

```toml
[tool.orchestra_dbt]
use_stateful = true
orchestra_env = "dev"
local_run = true
```

```bash
export ORCHESTRA_API_KEY=<API_KEY>
orc dbt run --target snowflake
```

You can still override with env vars (for example `ORCHESTRA_ENV=stage`) when needed.

Local JSON file (after creating the file as above): **unset** `ORCHESTRA_API_KEY` so `ORCHESTRA_STATE_FILE` or `state_file` in `pyproject.toml` is used.

```toml
[tool.orchestra_dbt]
use_stateful = true
state_file = ".orchestra/dbt_state.json"
local_run = true
```

```bash
orc dbt run --target snowflake
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

Without Postgres, the tutorial `dbt build` integration test is skipped. To run it locally, start Postgres, set `PGHOST`, `PGDATABASE`, and related variables (see `[tutorial/README.md](tutorial/README.md)`), then run `pytest tests/integration/test_tutorial_dbt.py`.

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
