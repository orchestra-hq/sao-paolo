import os
import subprocess
import sys
from importlib.metadata import version
from typing import cast

import click

from .constants import SERVICE_NAME, VALID_ORCHESTRA_ENVS
from .dag import construct_dag
from .logger import log_debug, log_error, log_info, log_reused_models
from .ls import get_model_paths_to_run
from .models import ModelNode, NodeType, SourceFreshness
from .modify import modify_dbt_command
from .orchestra import is_warn
from .patcher import patch_sql_files, revert_patching
from .sao import Freshness, calculate_models_to_run
from .source_freshness import get_source_freshness
from .state import load_state, save_state, update_state
from .target_finder import find_target_in_args


def _welcome() -> None:
    try:
        project_version = version(SERVICE_NAME)
    except Exception:
        project_version = "unknown"
    log_info(f"Version: {project_version}. Stateful orchestration enabled.")


def _validate_environment() -> None:
    if os.getenv("ORCHESTRA_ENV", "app").lower() not in VALID_ORCHESTRA_ENVS:
        log_error(
            f"Invalid ORCHESTRA_ENV environment variable. Must be one of: {', '.join(VALID_ORCHESTRA_ENVS)}"
        )
        sys.exit(1)

    if not os.getenv("ORCHESTRA_API_KEY"):
        log_error("Missing ORCHESTRA_API_KEY environment variable.")
        sys.exit(1)

    log_debug("Environment validated.")


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def main(args: tuple):
    if not args or args[0] != "dbt" or len(args) < 2:
        log_error("Usage: orchestra-dbt dbt [DBT_COMMAND] [ARGS...]")
        sys.exit(1)

    if args[1] == "orchestra":
        match args[2]:
            case "is_warn":
                is_warn()
            case _:
                log_error(f"dbt orchestra command {args[2]} not known.")
                sys.exit(1)
        sys.exit(0)

    if not os.getenv("ORCHESTRA_USE_STATEFUL", "false").lower() == "true":
        log_debug("Stateful orchestration is disabled. Running dbt command directly.")
        try:
            return subprocess.run(args).returncode
        except FileNotFoundError as file_not_found_error:
            log_error(
                f"dbt-core is not installed. Please install it. Issue: {file_not_found_error}"
            )
            sys.exit(1)

    if args[1] not in ["build", "run", "test"]:
        log_debug(f"dbt command {args[1]} not supported for stateful orchestration.")
        try:
            return subprocess.run(args).returncode
        except FileNotFoundError as file_not_found_error:
            log_error(
                f"dbt-core is not installed. Please install it. Issue: {file_not_found_error}"
            )
            sys.exit(1)

    _welcome()
    _validate_environment()

    try:
        model_paths_to_run: list[str] | None = get_model_paths_to_run(args[2:])
    except ImportError:
        sys.exit(1)

    source_freshness: SourceFreshness | None = get_source_freshness(
        target=find_target_in_args(list(args))
    )
    if not source_freshness:
        sys.exit(subprocess.run(args).returncode)
    log_info(f"Collected {len(source_freshness.sources)} source(s) information.")

    state = load_state()
    parsed_dag = construct_dag(source_freshness, state)

    # Edit the DAG inline.
    calculate_models_to_run(parsed_dag)

    models_to_reuse: dict[str, ModelNode] = {}
    models_count = 0
    for node_id, node in parsed_dag.nodes.items():
        if node.node_type == NodeType.SOURCE:
            continue
        model_node: ModelNode = cast(ModelNode, node)
        if model_paths_to_run and model_node.model_path not in model_paths_to_run:
            continue
        models_count += 1
        if model_node.freshness == Freshness.CLEAN:
            models_to_reuse[node_id] = model_node

    log_reused_models(models_to_reuse)

    if len(models_to_reuse) != 0:
        patch_sql_files(models_to_reuse)
        result = subprocess.run(modify_dbt_command(cmd=list(args)))
        log_info(f"{len(models_to_reuse)}/{models_count} models reused.")
        if os.getenv("ORCHESTRA_LOCAL_RUN", "false").lower() == "true":
            revert_patching(
                sql_paths_to_revert=[
                    model.sql_path for model in models_to_reuse.values()
                ]
            )
    else:
        result = subprocess.run(list(args))

    update_state(state=state, parsed_dag=parsed_dag, source_freshness=source_freshness)
    save_state(state=state)
    sys.exit(result.returncode)
