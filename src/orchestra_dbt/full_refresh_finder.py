import os

# dbt/click's exact truthy env var states (case-insensitive, trimmed).
_ENV_TRUE_STATES = {"1", "yes", "true", "on", "t", "y"}


def is_full_refresh_requested(args: list[str]) -> bool:
    """Resolve whether this dbt invocation runs with `--full-refresh`/`-f`/`DBT_FULL_REFRESH`.

    Unlike `--target`, this is a plain boolean flag with no value (click rejects
    `--full-refresh=true` outright), so presence of either flag always wins over the env var.
    Bundled short flags (`-fs model`) aren't unpicked -- rare enough to skip.
    """
    if "--full-refresh" in args or "-f" in args:
        return True

    return os.environ.get("DBT_FULL_REFRESH", "").strip().lower() in _ENV_TRUE_STATES
