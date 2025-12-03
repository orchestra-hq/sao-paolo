import threading
from datetime import datetime

from dbt.artifacts.schemas.freshness.v3.freshness import SourceFreshnessResult
from dbt.artifacts.schemas.results import FreshnessStatus
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.nodes import SourceDefinition
from dbt.task.freshness import FreshnessRunner, FreshnessTask
from dbt_common.exceptions import DbtRuntimeError

from orchestra_dbt.models import SourceFreshness

from .utils import load_file, log_info, log_warn


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


def get_source_freshness() -> SourceFreshness | None:
    log_info("Calculating source freshness")

    # Patching of this runs freshness for all defined sources in the dbt config.
    SourceDefinition.has_freshness = True  # pyright: ignore[reportAttributeAccessIssue]

    # Patch the execute method of the FreshnessRunner to still execute, but if it
    # fails and hits a dbt RuntimeError, return a base SourceFreshnessResult that is defined here.
    FreshnessTask.get_runner_type = lambda self, _: OrchestraFreshnessRunner

    try:
        dbtRunner().invoke(["source", "freshness", "-q"])
        return SourceFreshness(
            sources={
                source["unique_id"]: source["max_loaded_at"]
                for source in load_file("target/sources.json")["results"]
            }
        )
    except Exception as e:
        log_warn(f"Error running dbt source freshness (a source could be expired). {e}")
