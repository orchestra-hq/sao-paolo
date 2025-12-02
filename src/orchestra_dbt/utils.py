import json
import os
import sys

import click

from orchestra_dbt.models import ORCHESTRA_REUSED_NODE

SERVICE_NAME = "orchestra-dbt"


def _log(msg: str, fg: str | None):
    click.echo(click.style(f"[{SERVICE_NAME}] {msg}", fg=fg))


def log_info(msg):
    _log(msg, None)


def log_warn(msg):
    _log(msg, "yellow")


def log_error(msg):
    _log(f"ERROR: {msg}", "red")


def load_file(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def modify_dbt_command(cmd: list[str]) -> list[str]:
    cmd += ["--exclude", f"tag:{ORCHESTRA_REUSED_NODE}"]
    return cmd


def validate_environment():
    log_info("Validating environment (checking API key and cache key are set)")

    vals_to_check = ["ORCHESTRA_API_KEY", "ORCHESTRA_DBT_CACHE_KEY"]
    valid_orchestra_envs = ["app", "stage", "dev"]

    for val in vals_to_check:
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
