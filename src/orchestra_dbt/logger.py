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
    log_info(f"{len(nodes_to_reuse)} nodes to be reused.")
    for node_id in nodes_to_reuse.keys():
        log_debug(f" - {node_id}")
