# Tutorial dbt Core project

This folder is a minimal [dbt Core](https://www.getdbt.com/) project that works with Orchestra’s **state orchestration** for dbt Core: freshness, `build_after`, and skipping or reusing work based on upstream change signals.

## What is configured?

- **Seeds** (`dbt/seeds`) load CSV data into a **raw** schema. [`stg_events`](dbt/models/staging/stg_events.sql) uses `ref('raw_events')` so dbt runs the seed before staging (a `source()`-only reference can run in parallel with the seed and fail). **Sources** in [`dbt/models/schema.yml`](dbt/models/schema.yml) still document the logical raw layer and **source freshness** (`warn_after`, `loaded_at_field`) for Orchestra and the manifest.
- **Staging → intermediate → marts** show a small DAG; the mart [`mart_daily_totals`](dbt/models/marts/mart_daily_totals.sql) sets **`freshness.build_after`** so Orchestra can reason about when downstream models should run relative to upstream updates (see [dbt Core state management](https://docs.getorchestra.io/docs/guides/dbt-core-state-management/guide)).
- **Macros** ([`dbt/macros/audit_run_id.sql`](dbt/macros/audit_run_id.sql)) and **snapshots** ([`dbt/snapshots/snap_mart_daily_totals.sql`](dbt/snapshots/snap_mart_daily_totals.sql)) mirror common real projects without extra packages.

Orchestra’s CLI wrapper (`orchestra-dbt`) reads manifest and state from the platform when stateful orchestration is enabled (`use_stateful` in `[tool.orchestra_dbt]` or `ORCHESTRA_USE_STATEFUL=true`). See the repo root [README](../README.md) for configuration and env overrides.

## Local run (Postgres)

Ensure `uv sync --extra dev --extra adapters` has been run.

1. Start Postgres:

   ```bash
   docker run -d --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=tutorial -p 5432:5432 --name tutorial-postgres postgres:18
   ```

2. Export connection settings and schema (names can be adjusted):

   ```bash
   export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres PGDATABASE=tutorial
   export DBT_SCHEMA=sao_tutorial
   cd tutorial
   export DBT_PROFILES_DIR="$(pwd)/dbt"
   cd dbt && orc dbt build --target ci
   ```

`dbt/profiles.yml` uses only environment variables. Do not commit warehouse passwords.
