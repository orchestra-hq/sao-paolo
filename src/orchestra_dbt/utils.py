import json
import os

import yaml


def load_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def save_yaml(path: str, data: dict) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(data, f)


def get_integration_account_id_from_env() -> str | None:
    return os.getenv("ORCHESTRA_INTEGRATION_ACCOUNT_ID")
