# sao-paolo

## Installing

```bash
python3 -m venv .venv
source .venv/bin/activate
uv sync --extra dev
```

## Tutorial dbt project

A minimal dbt Core project used for docs and CI lives under [`tutorial/`](tutorial/). See [`tutorial/README.md`](tutorial/README.md) for Postgres setup, [`pipeline.yaml`](tutorial/pipeline.yaml) for an Orchestra pipeline template, and [`tutorial/dbt/`](tutorial/dbt/) for the models.

## Development

1. Create a branch
1. Add the code and unit tests
1. Where possible, [test locally](#running-locally)
1. Test in Orchestra [with the branch](#running-in-orchestra)
1. Raise a PR

Pull requests run GitHub Actions: unit tests, static checks, `dbt build` for `tutorial/dbt` against Postgres, and (when configured) an Orchestra pipeline via `orchestra-hq/run-pipeline`. Configure these repository secrets:

- `ORCHESTRA_API_KEY`
- `ORCHESTRA_PIPELINE_ID` — pipeline created from [`tutorial/pipeline.yaml`](tutorial/pipeline.yaml) (or equivalent) in Orchestra

The workflow uses the `production` environment for the Orchestra job (same pattern as the previous slim CI workflow).

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

Without Postgres, the tutorial `dbt build` integration test is skipped. To run it locally, start Postgres, set `PGHOST`, `PGDATABASE`, and related variables (see [`tutorial/README.md`](tutorial/README.md)), then run `pytest tests/integration/test_tutorial_dbt.py`.

For the optional DAG integration test, you need both `local_state.json` and `local_manifest.json` in the root directory. `local_state.json` can be created by running `dynamo_state.py` (see above), and `local_manifest.json` can be created by downloading a relevant dbt `manifest.json` file.

## Linting

```bash
ruff check . && ruff format --check . && basedpyright
```

To automatically fix issues:

```bash
ruff check --fix . && ruff format . && basedpyright
```
