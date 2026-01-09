import threading
from datetime import datetime

from .logger import log_error, log_info, log_warn
from .models import SourceFreshness
from .utils import load_json


def get_source_freshness(target: str | None) -> SourceFreshness | None:
    try:
        from dbt.artifacts.schemas.freshness.v3.freshness import (  # pyright: ignore[reportMissingImports]
            SourceFreshnessResult,
        )
        from dbt.artifacts.schemas.results import (  # pyright: ignore[reportMissingImports]
            FreshnessStatus,
        )
        from dbt.cli.main import dbtRunner  # pyright: ignore[reportMissingImports]
        from dbt.task.freshness import (  # pyright: ignore[reportMissingImports]
            FreshnessRunner,
            FreshnessTask,
        )
        from dbt_common.exceptions import (  # pyright: ignore[reportMissingImports]
            DbtRuntimeError,
        )
    except ImportError as missing_dbt_core_error:
        log_error(
            f"dbt-core is not installed. Please install it. Issue: {missing_dbt_core_error}"
        )
        raise missing_dbt_core_error

    class OrchestraFreshnessRunner(FreshnessRunner):
        def execute(self, compiled_node, manifest) -> SourceFreshnessResult:
            try:
                return super().execute(compiled_node, manifest)
            except DbtRuntimeError as e:
                log_warn(
                    f"Unable to calculate source freshness for {compiled_node.unique_id}: {e}"
                )
                return SourceFreshnessResult(
                    status=FreshnessStatus.Pass,
                    timing=[],
                    thread_id=threading.current_thread().name,
                    execution_time=0,
                    adapter_response={},
                    message=None,
                    failures=None,
                    node=compiled_node,
                    # Assume source is new if we can't calculate it.
                    max_loaded_at=datetime.now(),
                    snapshotted_at=datetime.now(),
                    age=0,
                )

    log_info("Calculating source freshness")

    # Patching of this runs freshness for all defined sources in the dbt config.
    # This can lead to erroneous results depending on the metadata of the warehouse.
    # SourceDefinition.has_freshness = True  # pyright: ignore[reportAttributeAccessIssue]

    # Patch the execute method of the FreshnessRunner to still execute, but if it
    # fails and hits a dbt RuntimeError, return a base SourceFreshnessResult that is defined here.
    FreshnessTask.get_runner_type = lambda self, _: OrchestraFreshnessRunner

    try:
        args: list[str] = ["source", "freshness", "-q"]
        if target:
            args.extend(["--target", target])
        dbtRunner().invoke(args=args)
        return SourceFreshness(
            sources={
                source["unique_id"]: source["max_loaded_at"]
                for source in load_json("target/sources.json")["results"]
            }
        )
    except Exception as e:
        log_warn(f"Error running dbt source freshness: {e}")
