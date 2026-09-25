from .compatibility import dbt_core_import_error_message
from .constants import RESOURCE_TYPES_TO_LS
from .logger import log_debug, log_error, log_info, log_warn

# Flags `dbt build`/`run`/`test` accept but `dbt ls` does not. Forwarding one makes dbt
# ls exit with "No such option", which costs us node-path discovery for the whole run.
DBT_LS_ARGS_NOT_ACCEPTED = [
    "--empty",
    "--no-empty",
    "--export-saved-queries",
    "--no-export-saved-queries",
    "--full-refresh",
    "-f",
    "--include-saved-query",
    "--no-include-saved-query",
    "--show",
    "--store-failures",
]
# Same, but these take a value, so the value has to be dropped with the flag.
DBT_LS_ARGS_NOT_ACCEPTED_WITH_VALUE = [
    "--event-time-start",
    "--event-time-end",
    "--sample",
    "--sqlparse",
    "--threads",
]


def get_args_for_ls(user_args: tuple) -> list[str]:
    command_args = ["ls"]
    resource_type_args = []
    for resource_type in RESOURCE_TYPES_TO_LS:
        resource_type_args.append("--resource-type")
        resource_type_args.append(resource_type)
    output_args = ["--output", "path", "-q"]

    # Remove args not accepted by dbt ls
    list_user_args = []
    skip_value = False
    for user_arg in user_args:
        if skip_value:
            skip_value = False
            continue
        # `--flag=value` is one token; `--flag value` is two.
        flag = user_arg.split("=", 1)[0]
        if flag in DBT_LS_ARGS_NOT_ACCEPTED:
            continue
        if flag in DBT_LS_ARGS_NOT_ACCEPTED_WITH_VALUE:
            skip_value = "=" not in user_arg
            continue
        list_user_args.append(user_arg)

    return command_args + resource_type_args + list_user_args + output_args


def get_paths_to_run(args: tuple) -> list[str] | None:
    try:
        from dbt.cli.main import (
            dbtRunner,
            dbtRunnerResult,
        )
    except ImportError as missing_dbt_core_error:
        log_error(dbt_core_import_error_message(missing_dbt_core_error))
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
        log_warn(f"Error getting [dbt ls] of nodes that will be executed: {e}")
        return None
