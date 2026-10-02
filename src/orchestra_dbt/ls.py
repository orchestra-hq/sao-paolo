import json
from typing import NamedTuple

from .compatibility import dbt_core_import_error_message
from .constants import RESOURCE_TYPES_TO_LS
from .logger import log_debug, log_error, log_info, log_warn

# Flags `dbt build`/`run`/`test` accept but `dbt ls` does not. Forwarding one makes dbt
# ls exit with "No such option", which costs us node-path discovery for the whole run.
# tests/unit/test_ls.py checks this against the installed dbt's own options.
DBT_LS_ARGS_NOT_ACCEPTED = {
    "--empty",
    "--no-empty",
    "--event-time-end",
    "--event-time-start",
    "--export-saved-queries",
    "--no-export-saved-queries",
    "--full-refresh",
    "-f",
    "--include-saved-query",
    "--no-include-saved-query",
    "--sample",
    "--show",
    "--sqlparse",
    "--store-failures",
    "--threads",
}


class NodesToRun(NamedTuple):
    """`paths` are matched against node paths elsewhere; `selectors` are dotted
    fqns for feeding back into `--select` (see get_args_for_source_freshness)."""

    paths: list[str]
    selectors: list[str]


def get_args_for_ls(user_args: tuple) -> list[str]:
    command_args = ["ls"]
    resource_type_args = []
    for resource_type in RESOURCE_TYPES_TO_LS:
        resource_type_args.append("--resource-type")
        resource_type_args.append(resource_type)
    # Both forms from one invocation -- asking twice re-resolves the same
    # selection. --output-keys is available from dbt 1.10.
    output_args = [
        "--output",
        "json",
        "--output-keys",
        "original_file_path",
        "fqn",
        "-q",
    ]

    # Drop unaccepted flags plus their values (no positionals, so bare tokens are values).
    list_user_args = []
    dropping = False
    for user_arg in user_args:
        if user_arg.startswith("-"):
            dropping = user_arg.split("=", 1)[0] in DBT_LS_ARGS_NOT_ACCEPTED
        if not dropping:
            list_user_args.append(user_arg)

    return command_args + resource_type_args + list_user_args + output_args


def get_nodes_to_run(args: tuple) -> NodesToRun | None:
    try:
        from dbt.cli.main import (
            dbtRunner,
            dbtRunnerResult,
        )
    except ImportError as missing_dbt_core_error:
        log_error(dbt_core_import_error_message(missing_dbt_core_error))
        raise

    log_info("Finding nodes to be executed:")

    try:
        res: dbtRunnerResult = dbtRunner().invoke(get_args_for_ls(args))
        if not res.success:
            raise ValueError(f"dbt ls failed to run correctly: {res.exception}")

        if isinstance(res.result, list) and all(
            isinstance(item, str) for item in res.result
        ):
            # One JSON object per node, carrying the keys asked for above. A missing
            # key raises, which the handler below turns into "couldn't resolve" --
            # better than silently returning two lists that disagree.
            nodes = [json.loads(line) for line in res.result]
            return NodesToRun(
                paths=[node["original_file_path"] for node in nodes],
                selectors=[".".join(node["fqn"]) for node in nodes],
            )

        raise ValueError(f"Unexpected result from dbt ls: {res.result}")
    except Exception as e:
        log_debug(e)

    log_warn("Error getting [dbt ls] of nodes that will be executed.")
    return None
