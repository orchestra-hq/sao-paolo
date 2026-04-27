# sao-paolo

## Introduction to State-Aware Orchestration for dbt ("Sao Paolo")

This repo contains the logic for running State-Aware Orchestration for dbt, or "Sao Paolo" for short.

There are a few core reasons to use Sao Paolo:

- Easier Scheduling: Orchestra SAO means you don’t need to manually tag models, you just need to say when the models should be updated and Orchestra handles the dependencies
- Save cost: Orchestra SAO detects when there is new data and only updates models and their downstream deps if there is new data, saving money and reducing time
- Works out the box: no need to upgrade dbt versions to take advantage of Orchestra SAO. Where you want to run SAO yourself, you can leverage the Sao Paolo repo which is open-source under Elastic V2 License.

## Getting Started with Sao Paolo

Head over to the [Sao Paolo pypi](https://pypi.org/project/dbt-orchestra/) package and `pip install dbt-orchestra`. 

## Installing and contributing to this repo

```bash
python3 -m venv .venv
source .venv/bin/activate
uv sync --extra dev
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
