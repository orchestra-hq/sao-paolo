# Tutorial dbt Core project

This folder is a minimal [dbt Core](https://www.getdbt.com/) project that works with Orchestra’s **state orchestration** for dbt Core: freshness, `build_after`, and skipping or reusing work based on upstream change signals.

## What is configured?

- **Seeds** (`dbt/seeds`) load CSV data into a **raw** schema. [`stg_events`](dbt/models/staging/stg_events.sql) uses `ref('raw_events')` so dbt runs the seed before staging (a `source()`-only reference can run in parallel with the seed and fail). **Sources** in [`dbt/models/schema.yml`](dbt/models/schema.yml) still document the logical raw layer and **source freshness** (`warn_after`, `loaded_at_field`) for Orchestra and the manifest.
- **Staging → intermediate → marts** show a small DAG; the mart [`mart_daily_totals`](dbt/models/marts/mart_daily_totals.sql) sets **`freshness.build_after`** so Orchestra can reason about when downstream models should run relative to upstream updates (see [dbt Core state management](https://docs.getorchestra.io/docs/guides/dbt-core-state-management/guide)).
- **Macros** ([`dbt/macros/audit_run_id.sql`](dbt/macros/audit_run_id.sql)) and **snapshots** ([`dbt/snapshots/snap_mart_daily_totals.sql`](dbt/snapshots/snap_mart_daily_totals.sql)) mirror common real projects without extra packages.

## Coverage of orchestration code paths

Use this tutorial project to exercise all supported stateful permutations:

| Scenario | Config / command | Behavior |
| --- | --- | --- |
| Stateful disabled pass-through | `ORCHESTRA_USE_STATEFUL=false` then `orc dbt build` | Wrapper delegates directly to dbt; no state orchestration. |
| Stateful with local file backend | `ORCHESTRA_USE_STATEFUL=true`, `ORCHESTRA_STATE_FILE=.orchestra/dbt_state.json`, no `ORCHESTRA_API_KEY` | State is loaded/saved from local JSON and clean nodes can be reused. |
| Stateful with HTTP backend | `ORCHESTRA_USE_STATEFUL=true`, `ORCHESTRA_API_KEY=...` | State is loaded/saved through Orchestra API. |
| Stateful with S3 backend | `ORCHESTRA_USE_STATEFUL=true`, `ORCHESTRA_STATE_FILE=s3://bucket/key`, no API key | State is loaded/saved in S3 (requires `orchestra-dbt[s3]`). |
| Full refresh override | any stateful backend + `orc dbt build --full-refresh` | Reuse logic is bypassed for that run; state is still updated after execution. |
| Supported stateful commands | `orc dbt run`, `orc dbt test` | Same orchestration flow as build: compute freshness, patch reusable nodes, update state. |
| Unsupported stateful command | `orc dbt seed` | Pass-through to dbt even when `use_stateful=true`. |

## Local run (Postgres)

Ensure `uv sync --extra dev --extra adapters` has been run.

1. Start Postgres:

   ```bash
   docker run -d --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=tutorial -p 5432:5432 --name tutorial-postgres postgres:18
   ```

1. Change to the tutorial/dbt directory:

   ```bash
   cd tutorial/dbt
   ```

1. If using local state file, create the file:

   ```bash
   mkdir -p .orchestra
   echo '{"state":{}}' > .orchestra/dbt_state.json
   ```

1. Export connection settings and schema (names can be adjusted):

   ```bash
   export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres PGDATABASE=tutorial DBT_SCHEMA=sao_tutorial DBT_PROFILES_DIR="$(pwd)"
   ```

1. Seed the database:

   ```bash
   orc dbt seed
   ```

   `orc dbt` commands other than `build`, `run` and `test` are passed through to `dbt`.

1. Run a dbt build for the first time:

   ```bash
   orc dbt build
   ```

   Logs will show that stateful orchestration is enabled, and that because the state file has no state in it, no nodes are re-used.
   The `.orchestra/dbt_state.json` file will have been populated for each node.

1. Run a dbt build again, now that we've hydrated the state file:

   ```bash
   orc dbt build
   ```

   This time, logs show that state was collected, and if the re-run happened within 5 minutes of the previous run, 3 nodes were re-used, avoiding unnecessary computation.

1. Amend the `models/schema.yaml` file's `config.freshness` blocks between `orc dbt build` runs, to see how this affects how models are re-used.

1. (Optional) To remove the Postgres container after testing, run:

   ```bash
   docker rm -f tutorial-postgres
   ```

`profiles.yml` uses only environment variables. Do not commit warehouse passwords.
