import json
from pathlib import Path
from .utils import log_info, log_warn

LOCAL_CACHE_DIR = Path.home() / ".orchestra"
LOCAL_CACHE_FILE = LOCAL_CACHE_DIR / "cache.json"


def _load_cache() -> dict:
    if LOCAL_CACHE_FILE.exists():
        try:
            return json.loads(LOCAL_CACHE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log_warn("Invalid cache file format — starting fresh.")
            return {}
    return {}


def _save_cache(cache: dict):
    LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    log_info(f"Cache updated: {LOCAL_CACHE_FILE}")


def get_entry(key: str | None) -> dict:
    if key is None:
        return {}
    cache = _load_cache()
    if key not in cache:
        log_warn(f"Cache entry not found: {key}")
        return {}
    return cache[key]


def set_entry(key: str, value: dict):
    cache = _load_cache()
    cache[key] = value
    _save_cache(cache)
