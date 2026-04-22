# Contributing

## Setup

Python 3.11+. From the repo root:

```bash
uv sync --extra dev
```

## Checks (same as CI)

```bash
uv run pytest
uv run ruff check . && uv run ruff format --check . && uv run basedpyright
```

Auto-fix: `uv run ruff check --fix . && uv run ruff format .`

## Pull requests

Target `main`. Keep changes focused; add or update tests when behaviour changes. Describe what changed and why.

Optional debug tooling is documented in `README.md` (`--extra debug`).

## License

This project is under the [Elastic License 2.0](LICENSE).
