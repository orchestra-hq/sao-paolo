# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Add durable project-specific notes here as they are discovered through real work.
- `dbt-core` and `sqlparse` (and everything else in the `dev` extra) are development/test-only dependencies, not runtime dependencies of the published `dbt-orchestra` package (see `dependencies` vs `[project.optional-dependencies] dev` in `pyproject.toml`). Vulnerabilities in them don't reach users installing from PyPI, but the dev pin still governs what `uv.lock` resolves to and can block transitive fixes (e.g. a `sqlparse` advisory that only clears once the `dbt-core` ceiling admits a newer minor).
- CI (`.github/workflows/ci.yaml`) matrices over Python versions only; there is no matrix over `dbt-core` versions, so bumping the `dbt-core` pin is tested against whatever single version `uv.lock` resolves to, not the whole advertised compatibility range in `README.md`.
- `uv lock` alone won't upgrade a transitive dependency just because its cap was raised — it prefers the existing resolution. Use `uv lock --upgrade-package <pkg>` (repeat per package) to force re-resolution after loosening a version constraint.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
