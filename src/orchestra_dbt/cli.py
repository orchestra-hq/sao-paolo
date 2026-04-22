import os
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path
from typing import cast

import click

from .build_after import propagate_freshness_config
from .compatibility import dbt_core_import_error_message
from .config import load_orchestra_dbt_settings, resolve_state_backend_config
from .state_types import StateBackendKind
from .constants import SERVICE_NAME
from .dag import construct_dag
from .logger import log_debug, log_error, log_info, log_reused_nodes
from .ls import get_paths_to_run
from .models import (
    MaterialisationNode,
    NodeType,
    ParsedDag,
    SourceFreshness,
    StateApiModel,
)
from .modify import modify_dbt_command
from .orchestra import is_warn
from .patcher import patch_seed_properties, patch_sql_files, revert_patching
from .sao import Freshness, calculate_nodes_to_run
from .source_freshness import get_source_freshness
from .state import StateLoadError, StateSaveError, load_state, save_state, update_state
from .target_finder import find_target_in_args


def _usage_program() -> str:
    return Path(sys.argv[0]).name if sys.argv else "orc"


def _welcome() -> None:
    try:
        project_version = version(distribution_name=SERVICE_NAME)
    except Exception:
        project_version = "unknown"
    log_info(f"Version: {project_version}. Stateful orchestration enabled.")


def _validate_environment() -> None:
    backend_cfg = resolve_state_backend_config()
    match backend_cfg.kind:
        case StateBackendKind.LOCAL_FILE:
            log_debug("State backend: local file (path configured).")
            return
        case StateBackendKind.S3:
            log_debug("State backend: S3 (URI configured).")
            return
        case StateBackendKind.HTTP:
            if not os.getenv("ORCHESTRA_API_KEY"):
                log_error(
                    "Stateful mode requires ORCHESTRA_API_KEY for Orchestra HTTP, or state storage "
                    "outside Orchestra: a local path, s3://bucket/key "
                    "(ORCHESTRA_STATE_FILE or [tool.orchestra_dbt] state_file in pyproject.toml)."
                )
                sys.exit(1)
            log_debug("State backend: Orchestra HTTP (API key set).")


def _complete_run(
    state: StateApiModel,
    parsed_dag: ParsedDag,
    source_freshness: SourceFreshness,
    dbt_exit_code: int,
) -> None:
    update_state(state=state, parsed_dag=parsed_dag, source_freshness=source_freshness)
    try:
        save_state(state=state)
    except StateSaveError as e:
        log_error(str(e))
        sys.exit(1)
    sys.exit(dbt_exit_code)


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def main(args: tuple[str, ...]) -> None:
    if not args:
        log_error(f"Usage: {_usage_program()} dbt <DBT_COMMAND> [ARGS...]")
        sys.exit(1)

    if args[0] != "dbt":
        log_error(
            f"Expected `{_usage_program()} dbt ...`. "
            f"Example: `{_usage_program()} dbt run` not `{_usage_program()} run`."
        )
        sys.exit(1)

    dbt_args: tuple[str, ...] = tuple(args)

    if len(dbt_args) < 2:
        log_error("dbt requires a subcommand (e.g. run, build, test).")
        sys.exit(1)

    if dbt_args[1] == "orchestra":
        if len(dbt_args) < 3:
            log_error("dbt orchestra requires a subcommand (e.g. is_warn).")
            sys.exit(1)
        match dbt_args[2]:
            case "is_warn":
                is_warn()
            case _:
                log_error(f"dbt orchestra command {dbt_args[2]} not known.")
                sys.exit(1)
        sys.exit(0)

    try:
        settings = load_orchestra_dbt_settings()
    except ValueError as exc:
        log_error(str(exc))
        sys.exit(1)

    if not settings.use_stateful:
        log_debug("Stateful orchestration is disabled. Running dbt command directly.")
        try:
            sys.exit(subprocess.run(dbt_args).returncode)
        except FileNotFoundError as file_not_found_error:
            log_error(
                f"dbt executable not found on PATH (install the dbt CLI). {file_not_found_error}"
            )
            sys.exit(1)

    if dbt_args[1] not in ["build", "run", "test"]:
        log_debug(
            f"dbt command {dbt_args[1]} not supported for stateful orchestration."
        )
        try:
            sys.exit(subprocess.run(dbt_args).returncode)
        except FileNotFoundError as file_not_found_error:
            log_error(
                f"dbt executable not found on PATH (install the dbt CLI). {file_not_found_error}"
            )
            sys.exit(1)

    _welcome()
    _validate_environment()

    try:
        paths_to_run: list[str] | None = get_paths_to_run(dbt_args[2:])
    except ImportError as import_error:
        log_error(dbt_core_import_error_message(import_error))
        sys.exit(1)

    try:
        source_freshness: SourceFreshness | None = get_source_freshness(
            target=find_target_in_args(list(dbt_args))
        )
    except ImportError as import_error:
        log_error(dbt_core_import_error_message(import_error))
        sys.exit(1)
    if not source_freshness:
        sys.exit(subprocess.run(dbt_args).returncode)
    log_info(f"Collected {len(source_freshness.sources)} source(s) information.")

    try:
        state = load_state()
    except StateLoadError as e:
        log_error(str(e))
        sys.exit(1)

    parsed_dag = construct_dag(source_freshness, state)

    # Propagate freshness config to upstream nodes
    propagate_freshness_config(parsed_dag)

    if "--full-refresh" in dbt_args:
        log_info("Full refresh detected. Stateful orchestration disabled.")
        _complete_run(
            state,
            parsed_dag,
            source_freshness,
            dbt_exit_code=subprocess.run(dbt_args).returncode,
        )

    # Edit the DAG inline.
    calculate_nodes_to_run(parsed_dag)

    nodes_to_reuse: dict[str, MaterialisationNode] = {}
    node_count = 0
    for node_id, node in parsed_dag.nodes.items():
        if node.node_type != NodeType.MATERIALISATION:
            continue
        materialisation_node: MaterialisationNode = cast(MaterialisationNode, node)
        if paths_to_run and materialisation_node.dbt_path not in paths_to_run:
            continue
        node_count += 1
        if materialisation_node.freshness == Freshness.CLEAN:
            nodes_to_reuse[node_id] = materialisation_node

    log_reused_nodes(nodes_to_reuse)

    if len(nodes_to_reuse) != 0:
        patch_sql_files(nodes_to_reuse)
        patch_seed_properties(nodes_to_reuse)

        result = subprocess.run(modify_dbt_command(cmd=list(dbt_args)))

        log_info(f"{len(nodes_to_reuse)}/{node_count} nodes reused.")
        if settings.local_run:
            revert_patching(
                file_paths_to_revert=[
                    node.file_path for node in nodes_to_reuse.values()
                ]
            )
    else:
        result = subprocess.run(list(dbt_args))

    _complete_run(state, parsed_dag, source_freshness, dbt_exit_code=result.returncode)
