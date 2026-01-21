from .constants import RESOURCE_TYPES_TO_LS
from .logger import log_debug, log_error, log_info, log_warn


def get_args_for_ls(user_args: tuple) -> list[str]:
    command_args = ["ls"]
    resource_type_args = []
    for resource_type in RESOURCE_TYPES_TO_LS:
        resource_type_args.append("--resource-type")
        resource_type_args.append(resource_type)
    output_args = ["--output", "path", "-q"]
    return command_args + resource_type_args + list(user_args) + output_args


def get_paths_to_run(args: tuple) -> list[str] | None:
    try:
        from dbt.cli.main import (  # pyright: ignore[reportMissingImports]
            dbtRunner,
            dbtRunnerResult,
        )
    except ImportError as missing_dbt_core_error:
        log_error(
            f"dbt-core is not installed. Please install it. Issue: {missing_dbt_core_error}"
        )
        raise missing_dbt_core_error

    log_info("Finding node paths to be executed:")

    try:
        res: dbtRunnerResult = dbtRunner().invoke(get_args_for_ls(args))
        if not res.success:
            raise ValueError(f"dbt ls failed to run correctly: {res.exception}")

        if isinstance(res.result, list) and all(
            isinstance(item, str) for item in res.result
        ):
            return res.result

        raise ValueError(f"Unexpected result from dbt ls: {res.result}")
    except Exception as e:
        log_debug(e)

    log_warn("Error getting list of nodes that will be executed.")
    return None
