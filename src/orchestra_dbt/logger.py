from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

import click

from .config import load_orchestra_dbt_settings
from .constants import SERVICE_NAME
from .models import MaterialisationNode


def _log(msg: str, fg: str | None, error: bool = False) -> None:
    text = str(datetime.now(UTC).strftime("%H:%M:%S")) + " "
    if error:
        text += " [ERROR]"
    text += f" [{SERVICE_NAME}] {msg}"
    click.echo(message=click.style(text=text, fg=fg), color=True)


def log_debug(msg) -> None:
    # The dbt project dir isn't known here, so only the cwd's pyproject.toml can enable this.
    if load_orchestra_dbt_settings(Path.cwd()).debug:
        _log(msg, None)


def log_info(msg) -> None:
    _log(msg, None)


def log_warn(msg) -> None:
    _log(msg, "yellow")


def log_error(msg) -> None:
    _log(msg, "red", error=True)


@contextmanager
def log_timed(label: str) -> Iterator[None]:
    started_at = perf_counter()
    try:
        yield
    finally:
        log_info(f"{label} took {perf_counter() - started_at:.2f}s.")


def log_reused_nodes(nodes_to_reuse: dict[str, MaterialisationNode]) -> None:
    total_nodes: int = len(nodes_to_reuse)
    log_info(f"{total_nodes} node(s) to be reused:")
    counter = 1
    for node_id, node in nodes_to_reuse.items():
        log_info(
            f"{counter} of {total_nodes} REUSED {node_id} - {node.reason} (last updated: {node.last_updated or 'none'})"
        )
        counter += 1
