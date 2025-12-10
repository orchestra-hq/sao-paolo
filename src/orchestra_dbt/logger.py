import os
from datetime import datetime

import click

from .constants import SERVICE_NAME
from .models import ModelNode


def _log(msg: str, fg: str | None):
    click.echo(
        click.style(
            f"{datetime.now().strftime('%H:%M:%S')} [{SERVICE_NAME}] {msg}", fg=fg
        )
    )


def log_debug(msg):
    if os.getenv("ORCHESTRA_DBT_DEBUG"):
        _log(f"[DEBUG] {msg}", None)


def log_info(msg):
    _log(msg, None)


def log_warn(msg):
    _log(msg, "yellow")


def log_error(msg):
    _log(f"[ERROR] {msg}", "red")


def log_reused_models(models_to_reuse: dict[str, ModelNode]):
    log_info(f"{len(models_to_reuse)} models to be reused.")
    for node_id in models_to_reuse.keys():
        log_debug(f" - {node_id}")
