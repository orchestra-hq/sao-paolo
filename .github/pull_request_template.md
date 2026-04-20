## Summary

<!-- What does this PR change and why? Link issues (e.g. ENG-1234) or discussions. -->

## How to test

<!-- Commands or scenarios reviewers can run. -->

```bash
uv run pytest
uv run ruff check . && uv run ruff format --check . && uv run basedpyright
```

## Checklist

- [ ] Tests added or updated where behaviour changed
- [ ] `uv run ruff check .`, `uv run ruff format --check .`, and `uv run basedpyright` pass locally
