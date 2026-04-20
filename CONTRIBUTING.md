# Contributing

Thanks for helping improve this project. This document covers how we work day to day and how we handle licensing for contributions.

## Development setup

- **Python:** 3.11+ (see `pyproject.toml` for policy).
- **Install:** use [uv](https://docs.astral.sh/uv/) from the repository root:

  ```bash
  uv sync --extra dev
  ```

- **Run tests:**

  ```bash
  uv run pytest
  ```

- **Lint and typecheck** (match CI):

  ```bash
  uv run ruff check . && uv run ruff format --check . && uv run basedpyright
  ```

  To auto-fix where possible:

  ```bash
  uv run ruff check --fix . && uv run ruff format . && uv run basedpyright
  ```

Optional tooling for debugging pipelines is described in `README.md` (`--extra debug` and related scripts).

## Pull requests

- Open PRs against `main` unless maintainers ask otherwise.
- Keep changes focused and include or update tests when behaviour changes.
- Describe **what** changed and **why**; link related issues or discussions when applicable.

### Developer Certificate of Origin (DCO)

This project uses the [Developer Certificate of Origin](https://developercertificate.org/) to record that contributors have the right to submit their work under the project license.

By contributing, you agree that your contribution is submitted under the DCO. **Sign every commit** with a line such as:

```text
Signed-off-by: Random J Developer <random@example.com>
```

You can add this automatically when committing with:

```bash
git commit -s
```

Squashed merge commits should retain sign-off on the commits that land in `main`.

### Contributor License Agreement (CLA)

Some contributors (for example employees of certain organisations) may need a separate **Contributor License Agreement** with Orchestra. If you are unsure, ask your legal or engineering contact before contributing substantial changes.

## Issues and security

- Use [GitHub Issues](https://github.com/getorchestra/sao-paolo/issues) for bug reports and feature ideas (use the templates when they fit).
- For **security vulnerabilities**, do not open a public issue. See [`SECURITY.md`](SECURITY.md).

## Code style

- Follow existing patterns in the codebase (naming, imports, structure).
- Prefer small, reviewable changes over large mixed refactors unless coordinated with maintainers.
