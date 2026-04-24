# dbt-orchestra

## Introduction

`orchestra-dbt` wraps dbt Core commands, use previous run state to reduce unnecessary work.

It is designed to be added to an existing dbt Core project, not used as a standalone dbt repository.

## Compatibility and prerequisites

- **Python:** 3.11, 3.12, and 3.13 only (see `requires-python` in `pyproject.toml`).
- **dbt-core:** 1.10.x and 1.11.x when using stateful orchestration.
- **A dbt Core project:** an existing dbt Core project where you already run `dbt build` / `dbt run` / `dbt test`.

## Installing

1. Install `orchestra-dbt` in the same environment as your dbt project:

    ```bash
    pip install orchestra-dbt
    ```

1. Add a minimal config block to your dbt project's `pyproject.toml`:

    ```toml
    [tool.orchestra_dbt]
    use_stateful = true
    state_file = ".orchestra/dbt_state.json"
    local_run = true
    ```

1. Bootstrap the local state file once:

    ```bash
    mkdir -p .orchestra
    echo '{"state":{}}' > .orchestra/dbt_state.json
    ```

1. Run your normal dbt command through `orc`:

    ```bash
    orc dbt run
    ```

After this run, your `.orchestra/dbt_state.json` state file will contain freshness information, and subsequent runs will compare this information to your project's model freshness configuration for optimisation.

If you want a small demo dbt project to try this with, use [`tutorial/README.md`](tutorial/README.md).

## State backends

### Local JSON file (quick start)

Local JSON is the easiest way to try state-aware orchestration quickly. Keep `ORCHESTRA_API_KEY` unset so `ORCHESTRA_STATE_FILE` or `state_file` in `pyproject.toml` is used.

```toml
[tool.orchestra_dbt]
use_stateful = true
state_file = ".orchestra/dbt_state.json"
local_run = true
```

```bash
orc dbt run
```

### Orchestra Cloud (managed)

Managing your dbt Core state in Orchestra requires an Orchestra API key. When `ORCHESTRA_API_KEY` is set, `orchestra-dbt` selects this backend, and ignores file-related settings. Put non-secret defaults in `pyproject.toml` and only export the API key:

```toml
[tool.orchestra_dbt]
use_stateful = true
local_run = true
```

```bash
export ORCHESTRA_API_KEY=<API_KEY>
orc dbt run
```

If you want to run state-aware dbt Core code without managing state files and the `orchestra-dbt` CLI tool, try running your dbt Core in Orchestra. Orchestra users can enable state-aware orchestration using a simple toggle.

### S3 backend

To store your dbt Core state in S3, install the optional dependency (`pip install 'orchestra-dbt[s3]'` or `uv sync --extra s3`). Credentials and region follow the usual [AWS SDK resolution](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) (environment variables, shared config, IAM role, etc.). If the object does not exist yet, load starts with an empty state and save creates the object.

## Daily usage

Stateful orchestration only runs for `dbt build`, `dbt run`, and `dbt test`. Other dbt subcommands are passed through to dbt unchanged.

### Runtime behavior by command and mode

| Stateful enabled | dbt command | Behavior |
| --- | --- | --- |
| `false` | any command | `orc` passes through to dbt with no state load/save. |
| `true` | `build`, `run`, `test` | `orc` loads state, computes reusable nodes, patches clean nodes, runs dbt, updates and saves state. |
| `true` | `build`, `run`, `test` + `--full-refresh` | `orc` skips reuse decisions for this invocation, runs dbt directly, then still updates/saves state after execution. |
| `true` | other command (for example `seed`, `docs generate`) | `orc` passes through to dbt unchanged. |

## Configuration reference

When stateful orchestration is enabled, the CLI loads and saves [dbt Core state](https://docs.getdbt.com/). Enable it with `use_stateful = true` under `[tool.orchestra_dbt]`, or set `ORCHESTRA_USE_STATEFUL=true`. That state is the same JSON shape regardless of the backend used.

**Do not put secrets in `pyproject.toml`.** Use environment variables (or your platform's secret store) for `ORCHESTRA_API_KEY`.

### Configuration precedence

For non-secret options, **if an environment variable is set, it overrides** values from `[tool.orchestra_dbt]`; otherwise the value from `pyproject.toml` is used, then the built-in default. The CLI discovers `pyproject.toml` by walking upward from the current working directory. `[tool.orchestra_dbt]` is read from that file when present.

### `[tool.orchestra_dbt]` options

| Key | Type | Default | Purpose |
| --- | --- | --- | --- |
| `state_file` | string (optional) | — | Local JSON path or `s3://bucket/key` for state (see backend table below). |
| `use_stateful` | bool | `false` | Turn on stateful orchestration for supported dbt commands. |
| `local_run` | bool | `true` | After reuse, revert patched files (typical for local iteration). |
| `debug` | bool | `false` | Verbose `orchestra-dbt` debug logging. |
| `integration_account_id` | string (optional) | — | When set, filter state keys to this integration account prefix. |

Equivalent environment overrides (when set): `ORCHESTRA_USE_STATEFUL`, `ORCHESTRA_LOCAL_RUN`, `ORCHESTRA_DBT_DEBUG`, `ORCHESTRA_INTEGRATION_ACCOUNT_ID`.

### Resolving multiple backend state configurations

| Priority | Setting | Effect |
| --- | --- | --- |
| 1 | `ORCHESTRA_API_KEY` | Load/save state via Orchestra HTTP. When the API key is set, `ORCHESTRA_STATE_FILE` and `state_file` in `pyproject.toml` are **ignored** for choosing the state backend. |
| 2 | `ORCHESTRA_STATE_FILE` | Path to a JSON file, or `s3://bucket/key` for an object in S3. Relative file paths are resolved from the current working directory. Used only when `ORCHESTRA_API_KEY` is unset. |
| 3 | `[tool.orchestra_dbt]` / `state_file` in `pyproject.toml` | Path to a JSON file, or `s3://bucket/key`. Relative file paths are resolved from the directory that contains the **discovered** `pyproject.toml`; absolute paths are used as-is. Used only when `ORCHESTRA_API_KEY` is unset and `ORCHESTRA_STATE_FILE` is unset. |

If an effective local path or S3 URI is configured (rows 2 or 3), that **file** or **S3** backend is used and an API key is not required for state. If `ORCHESTRA_API_KEY` is set (row 1), the **HTTP backend** is used regardless of file settings.

### Warehouse adapters and implicit source freshness

Stateful reuse uses `dbt source freshness` results. When a source defines **`loaded_at_field`** or **`loaded_at_query`**, dbt's normal freshness logic runs on every adapter Orchestra supports through dbt Core.

When **both** are omitted, Orchestra can still run **adapter-specific** SQL to infer `max_loaded_at` (see `src/orchestra_dbt/source_freshness/`). Only the adapters below register that path today; the mapping is keyed by `FreshnessRunner.adapter.type()`.

| Warehouse | dbt adapter type (typical) | Implicit freshness (no `loaded_at_*`) |
| --- | --- | --- |
| **Databricks** | `databricks` | **Supported** — uses `DESCRIBE HISTORY` on the source relation. |
| **Snowflake** | `snowflake` | **Use `loaded_at_field` or `loaded_at_query`** — no Orchestra fallback; standard dbt freshness. |
| **Microsoft Fabric** | `fabric` | Same as Snowflake — configure `loaded_at_*`; no Orchestra fallback. |
| **PostgreSQL** | `postgres` | Same as Snowflake — configure `loaded_at_*`; no Orchestra fallback. |
| **DuckDB** | `duckdb` | **Not supported** |
| **Other adapters** | varies | No Orchestra fallback unless listed above; use `loaded_at_*` or verify dbt's default behavior for your warehouse. |

For adapters without a registered fallback, if both `loaded_at` settings are missing, Orchestra follows dbt's `FreshnessRunner` behavior (which may surface as warnings or a non-actionable result depending on dbt and the warehouse).

### Example snippet

Example optional snippet in `pyproject.toml`:

```toml
[tool.orchestra_dbt]
use_stateful = true
state_file = ".orchestra/dbt_state.json"
```

Add `.orchestra/` (or your chosen path) to `.gitignore` if the file should not be committed.

## Development and contributing

For contributor guidance, see [`CONTRIBUTING.md`](CONTRIBUTING.md).
