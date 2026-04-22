import threading
from datetime import datetime
from typing import Any

import pytz


def parse_query_timestamp_cell(timestamp_value: object) -> datetime:
    if isinstance(timestamp_value, datetime):
        max_loaded_at = timestamp_value
    elif isinstance(timestamp_value, str):
        try:
            timestamp_str = timestamp_value.replace("Z", "+00:00")
            max_loaded_at = datetime.fromisoformat(timestamp_str)
        except ValueError:
            try:
                max_loaded_at = datetime.fromisoformat(timestamp_value)
            except ValueError as e:
                raise ValueError(f"Unable to parse timestamp: {timestamp_value}") from e
    else:
        raise ValueError(f"Unexpected timestamp type: {type(timestamp_value)}")

    if max_loaded_at.tzinfo is None:
        max_loaded_at = pytz.UTC.localize(max_loaded_at)
    return max_loaded_at


def build_source_freshness_result_from_loaded_at(
    *,
    max_loaded_at: datetime,
    compiled_node: Any,
    adapter_response: Any | None,
) -> Any:
    from dbt.artifacts.schemas.freshness.v3.freshness import SourceFreshnessResult
    from dbt.artifacts.schemas.results import FreshnessStatus

    snapshotted_at = datetime.now(pytz.UTC)
    age = (snapshotted_at - max_loaded_at).total_seconds()

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
        adapter_response=(
            adapter_response.to_dict(omit_none=True) if adapter_response else {}
        ),
        failures=None,
        max_loaded_at=max_loaded_at,
        snapshotted_at=snapshotted_at,
        age=age,
    )
