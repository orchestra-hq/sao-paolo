import sys
from datetime import datetime
from importlib.metadata import version

import click

from orchestra_dbt.dag import construct_dag
from orchestra_dbt.source_freshness import get_source_freshness

from .cache import load_state, save_state
from .dbt_runner import run_dbt_command
from .models import Node, NodeType, StateItem
from .patcher import patch_sql_files
from .sao import Freshness, calculate_models_to_run
from .utils import SERVICE_NAME, log_info, modify_dbt_command, validate_environment

STATE_AWARE_ENABLED = True


def _welcome():
    try:
        project_version = version(SERVICE_NAME)
    except Exception:
        project_version = "unknown"
    log_info(f"Version: {project_version}")


# -------------------------------
# Main CLI Group
# -------------------------------
@click.group()
def main():
    """
    Orchestra dbt CLI
    To be merged into one orchestra CLI/SDK tool in the future.
    """
    pass


# -------------------------------
# DBT Subcommand
# -------------------------------
@main.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
)
@click.argument("dbt_command", nargs=-1, type=click.UNPROCESSED)
def dbt(dbt_command):
    if not dbt_command:
        click.echo("Usage: orchestra-dbt dbt [DBT_COMMAND] [ARGS...]")
        sys.exit(1)

    _welcome()

    if not STATE_AWARE_ENABLED:
        log_info("Stateful orchestration disabled.")
        sys.exit(run_dbt_command(args=dbt_command).returncode)

    validate_environment()
    source_freshness = get_source_freshness()
    if not source_freshness:
        sys.exit(run_dbt_command(args=dbt_command).returncode)

    state = load_state()
    log_info("State loaded")

    parsed_dag = construct_dag(source_freshness, state)
    calculate_models_to_run(parsed_dag)

    models_to_reuse: list[Node] = []
    models_count = 0
    for node in parsed_dag.nodes.values():
        if node.type != NodeType.MODEL:
            continue
        models_count += 1
        if node.freshness == Freshness.CLEAN:
            models_to_reuse.append(node)
    log_info(f"Reusing {len(models_to_reuse)}/{models_count} models")

    patch_sql_files(models_to_reuse)
    result = run_dbt_command(modify_dbt_command(list[str](dbt_command)))

    for node_id, node in parsed_dag.nodes.items():
        state.state[node_id] = StateItem(
            checksum=node.checksum,
            last_updated=source_freshness.sources[node_id]
            if node_id in source_freshness.sources
            else datetime.now(),
        )
    save_state(state=state)
    log_info("State saved")
    sys.exit(result.returncode)
