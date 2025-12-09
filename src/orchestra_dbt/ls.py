from dbt.cli.main import dbtRunner, dbtRunnerResult

from .utils import log_debug, log_info, log_warn


def get_models_to_run(args: tuple) -> list[str] | None:
    log_info("Finding models to be executed:")

    try:
        res: dbtRunnerResult = dbtRunner().invoke(
            ["ls", "--resource-type", "model"] + list(args)
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
