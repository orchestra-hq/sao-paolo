from .utils import load_json


def is_warn() -> None:
    status = "SUCCEEDED"

    try:
        run_results = load_json(path="target/run_results.json")
        for result in run_results.get("results", []):
            if result.get("status") == "warn":
                status = "WARNING"
                break
    except Exception:
        pass

    print(status)
