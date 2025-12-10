from dbt.cli.main import dbtRunner, dbtRunnerResult

from .logger import log_debug, log_info, log_warn


def get_model_paths_to_run(args: tuple) -> list[str] | None:
    log_info("Finding model paths to be executed:")

    try:
        res: dbtRunnerResult = dbtRunner().invoke(
            ["ls", "--resource-type", "model"] + list(args) + ["--output", "path", "-q"]
        )
        if not res.success:
            raise ValueError(f"dbt ls failed to run correctly: {res.exception}")

        if isinstance(res.result, list) and all(
            isinstance(item, str) for item in res.result
        ):
            return res.result

        raise ValueError(f"Unexpected result from dbt ls: {res.result}")
    except Exception as e:
        log_debug(e)

    log_warn("Error getting list of models that will be executed.")
    return None
