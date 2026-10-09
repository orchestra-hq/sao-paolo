import os
from pathlib import Path


def find_target_in_args(args: list[str]) -> str | None:
    """Resolve the dbt target this run will use, matching click's real parsing rules.

    `-t` attaches with no `=` splitting (`-tprod` -> `"prod"`, `-t=prod` -> `"=prod"`), a
    repeated flag resolves to the last occurrence, and values are never stripped -- only a
    truly empty `DBT_TARGET` counts as unset, not a whitespace-only one.
    """
    target: str | None = None

    remaining = iter(args)
    for arg in remaining:
        if arg in ("--target", "-t"):
            target = next(remaining, target)
        elif arg.startswith("--target="):
            target = arg.removeprefix("--target=")
        elif arg.startswith("-t"):
            target = arg.removeprefix("-t")

    if target is not None:
        return target

    return os.environ.get("DBT_TARGET") or None


def find_flag_value(args: list[str], flag: str) -> str | None:
    """`--flag value` or `--flag=value`; the last occurrence wins, as in click."""
    value: str | None = None
    remaining = iter(args)
    for arg in remaining:
        if arg == flag:
            value = next(remaining, value)
        elif arg.startswith(f"{flag}="):
            value = arg.removeprefix(f"{flag}=")
    return value


def _dbt_setting(args: list[str], flag: str, name: str) -> str | None:
    return (
        find_flag_value(args, flag)
        or os.environ.get(f"DBT_ENGINE_{name}")
        or os.environ.get(f"DBT_{name}")
        or None
    )


def find_project_dir(args: list[str]) -> Path:
    """The dbt project dir: the flag beats DBT_ENGINE_* beats DBT_*."""
    return Path(_dbt_setting(args, "--project-dir", "PROJECT_DIR") or ".")
