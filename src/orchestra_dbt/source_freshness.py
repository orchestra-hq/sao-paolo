import threading
from datetime import datetime

import pytz

from .logger import log_error, log_info, log_warn
from .models import SourceFreshness
from .utils import load_json


def get_source_freshness(target: str | None) -> SourceFreshness | None:
    try:
        from dbt.artifacts.schemas.freshness import SourceDefinition
        from dbt.artifacts.schemas.freshness.v3.freshness import SourceFreshnessResult
        from dbt.artifacts.schemas.results import FreshnessStatus
        from dbt.cli.main import dbtRunner
        from dbt.task.freshness import FreshnessRunner, FreshnessTask
        from dbt_common.exceptions import DbtRuntimeError
    except ImportError as missing_dbt_core_error:
        log_error(
            f"dbt-core is not installed. Please install it. Issue: {missing_dbt_core_error}"
        )
        raise missing_dbt_core_error

    class OrchestraFreshnessRunner(FreshnessRunner):
        def execute(self, compiled_node, manifest) -> SourceFreshnessResult:
            if (
                compiled_node.loaded_at_query is None
                and compiled_node.loaded_at_field is None
                and self.adapter.type() == "databricks"
            ):
                try:
                    relation_path = self.adapter.Relation.create_from(
                        self.config, compiled_node
                    ).render()
                    query = f"SELECT timestamp FROM (DESCRIBE HISTORY {relation_path} LIMIT 1)"
                    log_info(
                        f"Using Databricks DESCRIBE HISTORY fallback for {compiled_node.unique_id}"
                    )

                    # Execute query within connection context
                    with self.adapter.connection_named(
                        compiled_node.unique_id, compiled_node
                    ):
                        self.adapter.clear_transaction()
                        adapter_response, table = self.adapter.execute(
                            sql=query, auto_begin=False, fetch=True
                        )

                        # Parse timestamp from result
                        if table and len(table.rows) > 0:
                            timestamp_value = table.rows[0][0]

                            # Handle different timestamp formats
                            if isinstance(timestamp_value, datetime):
                                max_loaded_at = timestamp_value
                            elif isinstance(timestamp_value, str):
                                # Try parsing ISO format timestamps
                                try:
                                    # Handle Z suffix and common ISO formats
                                    timestamp_str = timestamp_value.replace(
                                        "Z", "+00:00"
                                    )
                                    max_loaded_at = datetime.fromisoformat(
                                        timestamp_str
                                    )
                                except ValueError:
                                    # Try parsing without timezone info
                                    try:
                                        max_loaded_at = datetime.fromisoformat(
                                            timestamp_value
                                        )
                                    except ValueError:
                                        # If parsing fails, raise to fall back to standard execution
                                        raise ValueError(
                                            f"Unable to parse timestamp: {timestamp_value}"
                                        )
                            else:
                                raise ValueError(
                                    f"Unexpected timestamp type: {type(timestamp_value)}"
                                )

                            if max_loaded_at.tzinfo is None:
                                # Assume UTC if no timezone info
                                max_loaded_at = pytz.UTC.localize(max_loaded_at)
                            snapshotted_at = datetime.now(pytz.UTC)
                            age = (snapshotted_at - max_loaded_at).total_seconds()

                            # Determine status based on freshness threshold if it exists
                            if compiled_node.freshness:
                                status = compiled_node.freshness.status(age)
                            else:
                                status = FreshnessStatus.Pass

                            return SourceFreshnessResult(
                                node=compiled_node,
                                status=status,
                                thread_id=threading.current_thread().name,
                                timing=[],
                                execution_time=0,
                                message=None,
                                adapter_response=adapter_response.to_dict(
                                    omit_none=True
                                )
                                if adapter_response
                                else {},
                                failures=None,
                                max_loaded_at=max_loaded_at,
                                snapshotted_at=snapshotted_at,
                                age=age,
                            )
                        else:
                            log_warn(
                                f"No history found for {compiled_node.unique_id}, treating as new"
                            )
                except Exception as e:
                    log_warn(
                        f"Databricks DESCRIBE HISTORY fallback failed for {compiled_node.unique_id}: {e}. "
                        "Treating as new."
                    )
            else:
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
    SourceDefinition.has_freshness = True  # pyright: ignore[reportAttributeAccessIssue]

    # Patch the execute method of the FreshnessRunner to run custom execute.
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
