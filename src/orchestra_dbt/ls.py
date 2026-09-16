import json
from dataclasses import dataclass, field

from .compatibility import dbt_core_import_error_message
from .constants import RESOURCE_TYPES_TO_LS
from .logger import log_debug, log_error, log_info, log_warn

DBT_LS_ARGS_NOT_ACCEPTED = ["--empty"]


@dataclass(frozen=True)
class NodesToRun:
    """Which nodes this run will build, in both forms dbt reports them.

    `paths` are `original_file_path`s, matched against node paths elsewhere.
    `selectors` are dotted fqns, for feeding back into `--select`. Not
    interchangeable -- see get_args_for_source_freshness.
    """

    paths: list[str] = field(default_factory=list)
    selectors: list[str] = field(default_factory=list)


def get_args_for_ls(user_args: tuple) -> list[str]:
    command_args = ["ls"]
    resource_type_args = []
    for resource_type in RESOURCE_TYPES_TO_LS:
        resource_type_args.append("--resource-type")
        resource_type_args.append(resource_type)
    # Both keys in one invocation -- re-parsing a large project to get the other
    # form costs tens of seconds. --output-keys is available from dbt 1.10.
    output_args = [
        "--output",
        "json",
        "--output-keys",
        "original_file_path",
        "fqn",
        "-q",
    ]

    # Remove args not accepted by dbt ls
    list_user_args = []
    for user_arg in user_args:
        if user_arg in DBT_LS_ARGS_NOT_ACCEPTED:
            continue
        list_user_args.append(user_arg)

    return command_args + resource_type_args + list_user_args + output_args


def parse_ls_output(lines: list[str]) -> NodesToRun:
    """Split `dbt ls --output json` -- one JSON object per node, carrying the keys
    get_args_for_ls asked for -- into the two forms callers need."""
    paths: list[str] = []
    selectors: list[str] = []
    for line in lines:
        node = json.loads(line)
        path = node.get("original_file_path")
        fqn = node.get("fqn")
        if path:
            paths.append(path)
        if fqn:
            # dbt joins fqn parts with "." to make a selector; see its ListTask.
            selectors.append(".".join(fqn))
    return NodesToRun(paths=paths, selectors=selectors)


def get_nodes_to_run(args: tuple) -> NodesToRun | None:
    try:
        from dbt.cli.main import (
            dbtRunner,
            dbtRunnerResult,
        )
    except ImportError as missing_dbt_core_error:
        log_error(dbt_core_import_error_message(missing_dbt_core_error))
        raise missing_dbt_core_error

    log_info("Finding nodes to be executed:")

    try:
        res: dbtRunnerResult = dbtRunner().invoke(get_args_for_ls(args))
        if not res.success:
            raise ValueError(f"dbt ls failed to run correctly: {res.exception}")

        if isinstance(res.result, list) and all(
            isinstance(item, str) for item in res.result
        ):
            return parse_ls_output(res.result)

        raise ValueError(f"Unexpected result from dbt ls: {res.result}")
    except Exception as e:
        log_debug(e)

    log_warn("Error getting [dbt ls] of nodes that will be executed.")
    return None
