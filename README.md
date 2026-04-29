# dbt-orchestra

## Introduction

`dbt-orchestra` wraps dbt Core commands, using previous run state to reduce unnecessary work. It is designed to be added to an existing dbt Core project, not used as a standalone dbt repository.

There are a few core reasons to use this project:

- Easier Scheduling: Orchestra SAO (State Aware Orchestration) means you don’t need to manually tag models, you just need to say when the models should be updated and Orchestra handles the dependencies.
- Save cost: Orchestra SAO detects when there is new data and only updates models and their downstream deps if there is new data, saving money and reducing time.
- Works out of the box: no need to upgrade dbt versions to take advantage of Orchestra SAO

## Compatibility and prerequisites

- **Python:** 3.11, 3.12, and 3.13 only (see `requires-python` in `pyproject.toml`).
- **dbt-core:** 1.10.x and 1.11.x when using stateful orchestration.
- **A dbt Core project:** an existing dbt Core project where you already run `dbt build` / `dbt run` / `dbt test`.

## Installing

1. Install `dbt-orchestra` in the same environment as your dbt project:

    ```bash
    pip install dbt-orchestra
    ```

2. Add a minimal config block to your project's `pyproject.toml`:

    ```toml
    [tool.orchestra_dbt]
    use_stateful = true
    state_file = ".orchestra/dbt_state.json"
    ```

You can skip `pyproject.toml` entirely: every `[tool.orchestra_dbt]` option has an environment-variable override (see [Pyproject.toml and environment variables](#pyprojecttoml-and-environment-variables)).

## Running

1. Bootstrap the local state file once:

    ```bash
    mkdir -p .orchestra
    echo '{"state":{}}' > .orchestra/dbt_state.json
    ```

2. Run your normal dbt command through `orc`:

    ```bash
    orc dbt run
    ```

After this run, your `.orchestra/dbt_state.json` state file will contain freshness information, and subsequent runs will compare this information to your project's model freshness configuration for optimisation.

If you want a small demo dbt Core project to try this with, use [`tutorial/README.md`](tutorial/README.md).

## State backends

### Local JSON file (quick start)

Local JSON is the easiest way to try state-aware orchestration quickly. Keep `ORCHESTRA_API_KEY` unset so `ORCHESTRA_STATE_FILE` or `state_file` in `pyproject.toml` is used.

```toml
[tool.orchestra_dbt]
use_stateful = true
state_file = ".orchestra/dbt_state.json"
```

```bash
orc dbt run
```

### Orchestra Cloud (managed)

Managing your dbt Core state in Orchestra requires an Orchestra API key. When `ORCHESTRA_API_KEY` is set, `dbt-orchestra` selects this backend, and ignores file-related settings. Put non-secret defaults in `pyproject.toml` and only export the API key:

```toml
[tool.orchestra_dbt]
use_stateful = true
```

```bash
export ORCHESTRA_API_KEY=<API_KEY>
orc dbt run
```

If you want to run state-aware dbt Core code without managing state files and the `dbt-orchestra` CLI tool, try running your dbt Core in Orchestra. Orchestra users can enable state-aware orchestration using a simple toggle.

### S3 backend

To store your dbt Core state in S3, install the optional dependency (`pip install 'dbt-orchestra[s3]'` or `uv sync --extra s3`). Credentials and region follow the usual [AWS SDK resolution](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) (environment variables, shared config, IAM role, etc.). If the object does not exist yet, load starts with an empty state and save creates the object. The state file parameter expects a `s3://bucket/key` URI.

## Daily usage

Stateful orchestration only runs for `dbt build`, `dbt run`, and `dbt test`. Other dbt subcommands are passed through to dbt unchanged.

### Runtime behaviour by command and mode

| Stateful enabled | dbt command | Behaviour |
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

### Pyproject.toml and environment variables

Each `[tool.orchestra_dbt]` key can be set in TOML, or omitted and supplied only via the matching variable. When both are present, the environment variable wins (see [Configuration precedence](#configuration-precedence) above). `ORCHESTRA_API_KEY` has no TOML equivalent; it selects the Orchestra HTTP backend when set.

| `pyproject.toml` key | Environment variable |
| --- | --- |
| `state_file` | `ORCHESTRA_STATE_FILE` |
| `use_stateful` | `ORCHESTRA_USE_STATEFUL` |
| `local_run` | `ORCHESTRA_LOCAL_RUN` |
| `debug` | `ORCHESTRA_DBT_DEBUG` |
| `seed_state_orchestration` | `ORCHESTRA_SEED_STATE_ORCHESTRATION` |

For boolean settings, if the environment variable is **set**, the merged value is `true` only when the value is exactly the string `true` (case-insensitive); otherwise it is `false`. If the variable is **unset**, `pyproject.toml` (or the default) applies.

### `[tool.orchestra_dbt]` options

| Key | Type | Default | Purpose |
| --- | --- | --- | --- |
| `state_file` | string (optional) | — | Local JSON path or `s3://bucket/key` for state (see backend table below). |
| `use_stateful` | bool | `false` | Turn on stateful orchestration for supported dbt commands. |
| `local_run` | bool | `true` | After reuse, revert patched files (typical for local iteration). |
| `debug` | bool | `false` | Verbose logging. |
| `seed_state_orchestration` | bool | `false` | When `true`, seed nodes can be reused from state like models; when `false`, seeds are always treated as dirty for reuse. This feature should be considered experimental and may change in the future. |

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
| **Google BigQuery** | `bigquery` | Same as Snowflake — configure `loaded_at_*`; no Orchestra fallback. |
| **AWS Redshift** | `redshift` | Same as Snowflake — configure `loaded_at_*`; no Orchestra fallback. |
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

© 2026 Orchestra Technologies Limited. All rights reserved.

This source code is the property of Orchestra Technologies Limited. Unauthorized use, copying, modification, or distribution is prohibited.
