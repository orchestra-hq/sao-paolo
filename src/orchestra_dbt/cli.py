from datetime import datetime
import sys
import click
from orchestra_dbt.call_get_target import get_target_inner
from .api import get_pipelines, get_caches
from .config import (
    set_cache_id,
    get_cache_id,
    set_pipeline_id,
    get_pipeline_id,
    validate_environment,
)
from .dbt_runner import run_dbt_command
from .state import Freshness, compute_models_to_run
from .patcher import patch_sql_files
from .utils import log_info
from .cache_store import set_entry


# -------------------------------
# Main CLI Group
# -------------------------------
@click.group()
def main():
    """Orchestra dbt CLI"""
    pass


def modify_dbt_command(cmd: list[str]) -> list[str]:
    cmd += ["--exclude", "tag:reuse"]
    return cmd


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
        sys.exit(0)

    # Validate ORCHESTRA_API_KEY and cache set
    validate_environment()
    log_info("Pipeline ID: " + get_pipeline_id())
    log_info("Cache ID: " + get_cache_id())

    # Perform state aware orchestration.
    computed_nodes = compute_models_to_run()
    model_paths_to_update = [
        m["sql_path"]
        for m in computed_nodes.values()
        if m["freshness"] == Freshness.DIRTY and m["type"] == "model"
    ]
    log_info(f"Models to run: {', '.join(model_paths_to_update)}")

    # Patch dbt model .sql files
    patch_sql_files(model_paths_to_update)

    # Potentially edit the command
    final_cmd = modify_dbt_command(list[str](dbt_command))

    # Run dbt command under the hood
    run_dbt_command(final_cmd, passthrough=True)

    # Store cache
    set_entry(
        get_cache_id(),
        {
            k: {
                "checksum": v.get("checksum"),
                "last_updated": v.get(
                    "last_updated",
                    datetime.now().isoformat()
                    if v.get("sql_path") in model_paths_to_update
                    else None,
                ),
            }
            for k, v in computed_nodes.items()
        },
    )


# -------------------------------
# Cache Subcommands
# -------------------------------
@main.group()
def cache():
    """Manage cache configuration"""
    pass


@cache.command("set")
@click.argument("cache_id")
def cache_set(cache_id):
    set_cache_id(cache_id)
    click.echo(f"Cache ID set to {cache_id}")


@cache.command("current")
def cache_current():
    current = get_cache_id()
    if current:
        click.echo(f"Current cache_id: {current}")
    else:
        click.echo("No cache_id set")


@cache.command("list")
def cache_list():
    caches = get_caches()
    if not caches:
        click.echo("No caches found")
        return
    click.echo("Available caches:")
    for cache in caches:
        click.echo(cache)


# -------------------------------
# Pipeline Subcommands
# -------------------------------
@main.group()
def pipeline():
    """Manage pipeline configuration"""
    pass


@pipeline.command("set")
@click.argument("pipeline_id")
def pipeline_set(pipeline_id):
    set_pipeline_id(pipeline_id)
    click.echo(f"Pipeline ID set to {pipeline_id}")


@pipeline.command("current")
def pipeline_current():
    current = get_pipeline_id()
    if current:
        click.echo(f"Current pipeline_id: {current}")
    else:
        click.echo("No pipeline_id set")


@pipeline.command("list")
def pipeline_list():
    pipelines = get_pipelines()
    if not pipelines:
        click.echo("No pipelines found")
        return
    click.echo("Available pipelines:")
    for pipeline in pipelines:
        click.echo(f"{pipeline[0]}: {pipeline[1]}")

# -------------------------------
# Target Subcommands
# -------------------------------
@main.group()
def target():
    """Manage target configuration"""
    pass


@target.command("get")
def get_target():
    get_target_inner()
