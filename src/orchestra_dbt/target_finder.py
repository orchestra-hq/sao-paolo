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


_project_dir = Path(".")
_artifact_dir = Path("target")


def resolve_artifact_dir(args: list[str]) -> None:
    """Resolve the project and artifact dirs as dbt 1.12 and 2.0 do: flag, then
    DBT_ENGINE_*, then DBT_*; a relative target path sits under the project dir."""
    global _project_dir, _artifact_dir
    _project_dir = Path(_dbt_setting(args, "--project-dir", "PROJECT_DIR") or ".")
    target_path = _dbt_setting(args, "--target-path", "TARGET_PATH") or "target"
    _artifact_dir = _project_dir / target_path


def artifact_path(name: str) -> str:
    return str(_artifact_dir / name)


def project_path(relative: str) -> Path:
    """A project-relative path (e.g. a manifest `original_file_path`) as seen from the cwd."""
    return _project_dir / relative
