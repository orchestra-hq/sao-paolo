from contextlib import suppress

from .target_finder import artifact_path
from .utils import load_json


def is_warn() -> None:
    status = "SUCCEEDED"

    with suppress(Exception):
        run_results = load_json(path=artifact_path("run_results.json"))
        for result in run_results.get("results", []):
            if result.get("status") == "warn":
                status = "WARNING"
                break

    print(status)
