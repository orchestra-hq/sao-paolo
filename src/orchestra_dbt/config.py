import json
import os
from pathlib import Path
import sys
from .utils import log_error, log_info, log_warn

CONFIG_DIR = Path.home() / ".orchestra"
CONFIG_FILE = CONFIG_DIR / "config.json"


def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log_warn("Invalid config file, starting fresh.")
            return {}
    return {}


def save_config(config: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")
    log_info(f"Saved config to {CONFIG_FILE}")


def get_attr(key: str, default=None):
    config = load_config()
    return config.get(key, default)


def set_attr(key: str, value):
    config = load_config()
    config[key] = value
    save_config(config)


def get_cache_id() -> str | None:
    return get_attr("cache_id")


def set_cache_id(cache_id):
    set_attr("cache_id", cache_id)


def get_pipeline_id():
    return get_attr("pipeline_id")


def set_pipeline_id(pipeline_id):
    set_attr("pipeline_id", pipeline_id)


def validate_environment():
    if not os.getenv("ORCHESTRA_API_KEY"):
        log_error("Missing ORCHESTRA_API_KEY environment variable.")
        sys.exit(1)
    if not get_pipeline_id():
        log_error("No pipeline_id set. Run `orchestra-dbt pipeline set <id>`.")
        sys.exit(1)
    if not get_cache_id():
        log_error("No cache_id set. Run `orchestra-dbt cache set <id>`.")
        sys.exit(1)
