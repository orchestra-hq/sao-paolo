# sao-paolo

## Installing

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

##  Debugging

To debug pipelines, there are some local files and scripts.

To install the required dependencies, run:

```bash
uv sync --extra dev --extra debug
```

Ask @ojc-orchestra for access to the scripts.

## Testing

```bash
pytest
```

## Linting

```bash
ruff check . && ruff format --check . && basedpyright
```

To automatically fix issues:

```bash
ruff check --fix . && ruff format . && basedpyright
```
