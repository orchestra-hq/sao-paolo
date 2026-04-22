import threading
from datetime import datetime

from ..compatibility import dbt_core_import_error_message
from ..logger import log_error, log_info, log_warn
from ..models import SourceFreshness
from ..utils import load_json
from .fallbacks.registry import FALLBACK_BY_ADAPTER_TYPE, loaded_at_fields_unset


def get_source_freshness(target: str | None) -> SourceFreshness | None:
    try:
        from dbt.artifacts.schemas.freshness import SourceDefinition
        from dbt.artifacts.schemas.freshness.v3.freshness import SourceFreshnessResult
        from dbt.artifacts.schemas.results import FreshnessStatus
        from dbt.cli.main import dbtRunner
        from dbt.task.freshness import FreshnessRunner, FreshnessTask
        from dbt_common.exceptions import DbtRuntimeError
    except ImportError as missing_dbt_core_error:
        log_error(dbt_core_import_error_message(missing_dbt_core_error))
        raise missing_dbt_core_error

    def default_freshness_result(compiled_node) -> SourceFreshnessResult:
        return SourceFreshnessResult(
            status=FreshnessStatus.Pass,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            adapter_response={},
            message=None,
            failures=None,
            node=compiled_node,
            max_loaded_at=datetime.now(),
            snapshotted_at=datetime.now(),
            age=0,
        )

    class OrchestraFreshnessRunner(FreshnessRunner):
        def execute(self, compiled_node, manifest) -> SourceFreshnessResult:
            if loaded_at_fields_unset(compiled_node):
                handler = FALLBACK_BY_ADAPTER_TYPE.get(self.adapter.type())
                if handler:
                    res = handler(self, compiled_node, manifest)
                    if res is not None:
                        return res
                    return default_freshness_result(compiled_node)

            try:
                return super().execute(compiled_node, manifest)
            except DbtRuntimeError as e:
                log_warn(
                    f"Unable to calculate source freshness for {compiled_node.unique_id}: {e}"
                )
            return default_freshness_result(compiled_node)

    log_info("Calculating source freshness")

    SourceDefinition.has_freshness = True  # pyright: ignore[reportAttributeAccessIssue]
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
