import subprocess
import sys
from datetime import datetime
from importlib.metadata import version

import click

from orchestra_dbt.dag import construct_dag
from orchestra_dbt.source_freshness import get_source_freshness

from .cache import load_state, save_state
from .models import Node, NodeType, StateItem
from .patcher import patch_sql_files
from .sao import Freshness, calculate_models_to_run
from .utils import (
    SERVICE_NAME,
    log_error,
    log_info,
    modify_dbt_command,
    validate_environment,
)

STATE_AWARE_ENABLED = True


def _welcome():
    try:
        project_version = version(SERVICE_NAME)
    except Exception:
        project_version = "unknown"
    log_info(f"Version: {project_version}")


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def main(args):
    if not args or args[0] != "dbt" or len(args) < 2:
        log_error("Usage: orchestra-dbt dbt [DBT_COMMAND] [ARGS...]")
        sys.exit(1)

    if args[1] not in ["build", "run", "test"]:
        # No stateful orchestration to perform on other dbt commands.
        sys.exit(subprocess.run(args).returncode)

    _welcome()
    if not STATE_AWARE_ENABLED:
        log_info("Stateful orchestration disabled.")
        sys.exit(subprocess.run(args).returncode)

    validate_environment()
    source_freshness = get_source_freshness()
    if not source_freshness:
        sys.exit(subprocess.run(args).returncode)

    state = load_state()

    parsed_dag = construct_dag(source_freshness, state)
    calculate_models_to_run(parsed_dag)

    models_to_reuse: dict[str, Node] = {}
    models_count = 0
    for node_id, node in parsed_dag.nodes.items():
        if node.type != NodeType.MODEL:
            continue
        models_count += 1
        if node.freshness == Freshness.CLEAN:
            models_to_reuse[node_id] = node

    log_info("Models to be reused:")
    for node_id in models_to_reuse.keys():
        log_info(f" - {node_id}")

    patch_sql_files(list(models_to_reuse.values()))
    result = subprocess.run(modify_dbt_command(cmd=list(args)))
    log_info(f"{len(models_to_reuse)}/{models_count} models reused.")

    for node_id, node in parsed_dag.nodes.items():
        state.state[node_id] = StateItem(
            checksum=node.checksum,
            last_updated=source_freshness.sources[node_id]
            if node_id in source_freshness.sources
            else datetime.now(),
        )
    save_state(state=state)
    sys.exit(result.returncode)
