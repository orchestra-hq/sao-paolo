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


def profile_args(user_args: list[str]) -> list[str]:
    """The flags a helper dbt invocation needs to resolve the same profile as the run."""
    args: list[str] = []
    if target := find_target_in_args(user_args):
        args.extend(["--target", target])
    for flag in ("--profiles-dir", "--profile", "--vars"):
        if (value := find_flag_value(user_args, flag)) is not None:
            args.extend([flag, value])
    return args
