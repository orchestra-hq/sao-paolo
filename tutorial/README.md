# Stateful dbt Core tutorial (sao-paolo)

This folder is a minimal [dbt Core](https://www.getdbt.com/) project that works with Orchestra’s **state orchestration** for dbt Core: freshness, `build_after`, and skipping or reusing work based on upstream change signals.

## Layout

| Path | Purpose |
|------|---------|
| [`README.md`](README.md) | This document |
| [`dbt/`](dbt/) | dbt project (`dbt_project.yml`, models, seeds, snapshots, macros, `profiles.yml`) |
| [`pipeline.yaml`](pipeline.yaml) | Example Orchestra pipeline (import or compare with your exported YAML) |

## What is configured here

- **Seeds** (`dbt/seeds`) load CSV data into a **raw** schema. [`stg_events`](dbt/models/staging/stg_events.sql) uses `ref('raw_events')` so dbt runs the seed before staging (a `source()`-only reference can run in parallel with the seed and fail). **Sources** in [`dbt/models/schema.yml`](dbt/models/schema.yml) still document the logical raw layer and **source freshness** (`warn_after`, `loaded_at_field`) for Orchestra and the manifest.
- **Staging → intermediate → marts** show a small DAG; the mart [`mart_daily_totals`](dbt/models/marts/mart_daily_totals.sql) sets **`freshness.build_after`** so Orchestra can reason about when downstream models should run relative to upstream updates (see [dbt Core state management](https://docs.getorchestra.io/docs/guides/dbt-core-state-management/guide)).
- **Macros** ([`dbt/macros/audit_run_id.sql`](dbt/macros/audit_run_id.sql)) and **snapshots** ([`dbt/snapshots/snap_mart_daily_totals.sql`](dbt/snapshots/snap_mart_daily_totals.sql)) mirror common real projects without extra packages.

Orchestra’s CLI wrapper (`orchestra-dbt`) reads manifest and state from the platform when `ORCHESTRA_USE_STATEFUL=true`. See the repo root [README](../README.md) for local env vars.

## Local run (Postgres)

1. Start Postgres (example):

   ```bash
   docker run --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=tutorial -p 5432:5432 postgres:16
   ```

2. Export connection settings and schema (names can be adjusted):

   ```bash
   export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres PGDATABASE=tutorial
   export DBT_SCHEMA=sao_tutorial
   export DBT_PROFILES_DIR="$(pwd)/dbt"
   cd dbt && dbt build --target ci
   ```

`dbt/profiles.yml` uses only environment variables—do not commit warehouse passwords.

## CI and Orchestra

GitHub Actions runs:

1. Unit tests and static checks on the Python package.
2. **`dbt build`** against a Postgres service using `tutorial/dbt`.
3. **`orchestra-hq/run-pipeline`** so an Orchestra pipeline executes against the **current PR branch** (see workflow). Configure repository secrets:

   - `ORCHESTRA_API_KEY`
   - `ORCHESTRA_PIPELINE_ID` — pipeline created from [`pipeline.yaml`](pipeline.yaml) (or equivalent) in Orchestra.

Run inputs pass `dbt_branch` so the integration can install and run this repository at the same branch Orchestra is asked to run. Validate run outcomes in the Orchestra UI as needed.

## Further reading

- [Pipeline YAML schema](https://docs.getorchestra.io/docs/core-concepts/pipelines/schema)
- [dbt Core integration](https://docs.getorchestra.io/docs/integrations/dbt_core)
- [State management tutorial](https://docs.getorchestra.io/docs/guides/dbt-core-state-management/tutorial)
