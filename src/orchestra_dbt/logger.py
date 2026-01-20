import os
from datetime import datetime

import click

from .constants import SERVICE_NAME
from .models import MaterialisationNode


def _log(msg: str, fg: str | None, error: bool = False) -> None:
    text = str(datetime.now().strftime("%H:%M:%S")) + " "
    if error:
        text += " [ERROR]"
    text += f" [{SERVICE_NAME}] {msg}"
    click.echo(message=click.style(text=text, fg=fg), color=True)


def log_debug(msg) -> None:
    if os.getenv("ORCHESTRA_DBT_DEBUG"):
        _log(msg, None)


def log_info(msg) -> None:
    _log(msg, None)


def log_warn(msg) -> None:
    _log(msg, "yellow")


def log_error(msg) -> None:
    _log(msg, "red", error=True)


def log_reused_nodes(nodes_to_reuse: dict[str, MaterialisationNode]) -> None:
    total_nodes: int = len(nodes_to_reuse)
    log_info(f"{total_nodes} node(s) to be reused:")
    counter = 1
    for node_id, node in nodes_to_reuse.items():
        log_info(
            f"{counter} of {total_nodes} REUSED {node_id} - {node.reason}. Last updated: {node.last_updated or 'none'}"
        )
        counter += 1
