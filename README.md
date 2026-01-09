# sao-paolo

## Installing

```bash
python3 -m venv .venv
source .venv/bin/activate
uv sync --extra dev
```

## Running

```bash
ORCHESTRA_ENV=dev ORCHESTRA_API_KEY=<API_KEY> ORCHESTRA_USE_STATEFUL=true ORCHESTRA_LOCAL_RUN=true orchestra-dbt dbt run --target snowflake
```

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