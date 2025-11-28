# sao-paolo

## Installing

```bash
python3 -m venv .venv
source .venv/bin/activate
uv sync --extra dev
```

## Running

```bash
orchestra-dbt dbt run
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
