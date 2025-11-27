import json
import click


def log_info(msg):
    click.echo(click.style(f"[orchestra-dbt] {msg}"))


def log_success(msg):
    click.echo(click.style(f"[orchestra-dbt] {msg}", fg="green"))


def log_warn(msg):
    click.echo(click.style(f"[orchestra-dbt] {msg}", fg="yellow"))


def log_error(msg):
    click.echo(click.style(f"[orchestra-dbt] ERROR: {msg}", fg="red"), err=True)


def load_file(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)
