import sys
from datetime import datetime

import click

from .cache import load_state, save_state
from .dbt_runner import run_dbt_command
from .models import NodeType, StateItem
from .patcher import patch_sql_files
from .state import (
    Freshness,
    calculate_models_to_run,
    construct_dag,
    get_source_freshness,
)
from .utils import log_info, modify_dbt_command, validate_environment

STATE_AWARE_ENABLED = True


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

    if not STATE_AWARE_ENABLED:
        sys.exit(run_dbt_command(args=dbt_command).returncode)

    validate_environment()
    source_freshness = get_source_freshness()
    state = load_state()
    log_info("State loaded")

    parsed_dag = construct_dag(source_freshness, state)
    calculate_models_to_run(parsed_dag, state)

    model_paths_to_update = [
        m.sql_path
        for m in parsed_dag.nodes.values()
        if m.freshness == Freshness.DIRTY and m.type == NodeType.MODEL and m.sql_path
    ]
    log_info(f"Models to run: {', '.join(model_paths_to_update)}")

    patch_sql_files(model_paths_to_update)
    result = run_dbt_command(modify_dbt_command(list[str](dbt_command)))

    for node_id, node in parsed_dag.nodes.items():
        state.state[node_id] = StateItem(
            checksum=node.checksum,
            last_updated=source_freshness.sources[node_id]
            if node_id in source_freshness.sources
            else datetime.now(),
        )
    save_state(state=state)
    sys.exit(result.returncode)
