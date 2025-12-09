import json
import os
import sys
from datetime import datetime

import click
import yaml

SERVICE_NAME = "orchestra-dbt"


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


def load_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def save_yaml(path: str, data: dict) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(data, f)


def validate_environment() -> None:
    valid_orchestra_envs: list[str] = ["app", "stage", "dev"]
    if os.getenv("ORCHESTRA_ENV", "app").lower() not in valid_orchestra_envs:
        log_error(
            f"Invalid ORCHESTRA_ENV environment variable. Must be one of: {', '.join(valid_orchestra_envs)}"
        )
        sys.exit(1)

    if not os.getenv("ORCHESTRA_API_KEY"):
        log_error("Missing ORCHESTRA_API_KEY environment variable.")
        sys.exit(1)

    log_debug("Environment validated.")


def get_base_api_url() -> str:
    return f"https://{os.getenv('ORCHESTRA_ENV', 'app').lower()}.getorchestra.io/api/engine/public"


def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {os.getenv('ORCHESTRA_API_KEY')}",
    }
