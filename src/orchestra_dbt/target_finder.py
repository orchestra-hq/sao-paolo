import os


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
