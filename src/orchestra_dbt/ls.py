from .logger import log_debug, log_error, log_info, log_warn


def get_model_paths_to_run(args: tuple) -> list[str] | None:
    try:
        from dbt.cli.main import (  # pyright: ignore[reportMissingImports]
            dbtRunner,
            dbtRunnerResult,
        )
    except ImportError as missing_dbt_core_error:
        log_error(
            f"dbt-core is not installed. Please install it. Issue: {missing_dbt_core_error}"
        )
        raise missing_dbt_core_error

    log_info("Finding model paths to be executed:")

    try:
        res: dbtRunnerResult = dbtRunner().invoke(
            ["ls", "--resource-type", "model, snapshot"]
            + list(args)
            + ["--output", "path", "-q"]
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
