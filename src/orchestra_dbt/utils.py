import json
from pathlib import Path

import yaml


def load_json(path: str | Path) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def load_yaml(path: str | Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def save_yaml(path: str | Path, data: dict) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(data, f)


def load_seed_bytes(path: str | Path) -> bytes:
    with open(path, "rb") as f:
        return f.read()
