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


def validate_environment():
    log_info("Validating environment (checking API key and cache key are set)")

    valid_orchestra_envs: list[str] = ["app", "stage", "dev"]

    for val in ["ORCHESTRA_API_KEY", "ORCHESTRA_DBT_CACHE_KEY"]:
        if not os.getenv(val):
            log_error(f"Missing {val} environment variable.")
            sys.exit(2)

    if os.getenv("ORCHESTRA_ENV", "app").lower() not in valid_orchestra_envs:
        log_error(
            f"Invalid ORCHESTRA_ENV environment variable. Must be one of: {', '.join(valid_orchestra_envs)}"
        )
        sys.exit(2)


def get_base_api_url() -> str:
    return f"https://{os.getenv('ORCHESTRA_ENV', 'app').lower()}.getorchestra.io/api/engine/public"


def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {os.getenv('ORCHESTRA_API_KEY')}",
    }
