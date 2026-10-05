import json
from typing import NamedTuple

from .compatibility import dbt_core_import_error_message
from .constants import RESOURCE_TYPES_TO_LS
from .logger import log_debug, log_error, log_info, log_warn
from .utils import load_json

# Build/run/test flags dbt ls rejects ("No such option"); checked in test_ls.py.
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


def _nodes_from_ls_result(items: list[str]) -> NodesToRun:
    """Turn `dbt ls` output into both forms, whichever shape dbt gave us.

    dbt 1.x honours `--output json`, so each item is a JSON object carrying the keys
    asked for. dbt 2.x applies `--output` only to stdout -- the programmatic result is
    always a list of dotted fqns whatever is asked for -- so there the fqn is already
    the selector, and only the path has to be recovered, from the manifest `dbt ls`
    has just written.
    """
    if not items:
        return NodesToRun(paths=[], selectors=[])

    if items[0].startswith("{"):
        # One JSON object per node, carrying the keys asked for above. A missing
        # key raises, which the handler below turns into "couldn't resolve" --
        # better than silently returning two lists that disagree.
        nodes = [json.loads(line) for line in items]
        return NodesToRun(
            paths=[node["original_file_path"] for node in nodes],
            selectors=[".".join(node["fqn"]) for node in nodes],
        )

    manifest_nodes = load_json("target/manifest.json")["nodes"]
    by_fqn = {
        ".".join(node["fqn"]): node["original_file_path"]
        for node in manifest_nodes.values()
    }
    # Dropped from both lists together, so they cannot disagree.
    resolved = [(by_fqn[fqn], fqn) for fqn in items if fqn in by_fqn]
    if unresolved := len(items) - len(resolved):
        log_warn(
            f"{unresolved} of {len(items)} node(s) from dbt ls are not in the manifest."
        )
    log_debug(f"Resolved {len(resolved)} dbt ls fqn(s) to file paths via the manifest.")
    return NodesToRun(
        paths=[path for path, _ in resolved],
        selectors=[fqn for _, fqn in resolved],
    )


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
            return _nodes_from_ls_result(res.result)

        raise ValueError(f"Unexpected result from dbt ls: {res.result}")
    except Exception as e:
        log_debug(e)
        log_warn(f"Error getting [dbt ls] of nodes that will be executed: {e}")
        return None
