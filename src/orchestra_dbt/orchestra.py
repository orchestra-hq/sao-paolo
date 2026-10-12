from contextlib import suppress
from pathlib import Path

from .utils import load_json


def is_warn(target_dir: Path = Path("target")) -> None:
    status = "SUCCEEDED"

    with suppress(Exception):
        run_results = load_json(target_dir / "run_results.json")
        for result in run_results.get("results", []):
            if result.get("status") == "warn":
                status = "WARNING"
                break

    print(status)
