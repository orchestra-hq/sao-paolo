import os
from datetime import datetime

import click

from .constants import SERVICE_NAME
from .models import ModelNode


def _log(msg: str, fg: str | None, error: bool = False) -> None:
    text = f"[{str(datetime.now().strftime('%H:%M:%S'))}]"
    if error:
        text += " [ERROR]"
    text += f" [{SERVICE_NAME}] {msg}"
    click.echo(message=click.style(text=text, fg=fg))


def log_debug(msg):
    if os.getenv("ORCHESTRA_DBT_DEBUG"):
        _log(f"[DEBUG] {msg}", None)


def log_info(msg):
    _log(msg, None)


def log_warn(msg):
    _log(msg, "yellow")


def log_error(msg):
    _log(msg, "red", error=True)


def log_reused_models(models_to_reuse: dict[str, ModelNode]):
    log_info(f"{len(models_to_reuse)} models to be reused.")
    for node_id in models_to_reuse.keys():
        log_debug(f" - {node_id}")
