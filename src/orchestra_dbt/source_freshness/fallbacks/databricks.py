from typing import Any

from ...logger import log_info, log_warn
from .common import (
    build_source_freshness_result_from_loaded_at,
    parse_query_timestamp_cell,
)


def try_databricks_fallback(runner: Any, compiled_node: Any, _manifest: Any) -> Any:
    try:
        relation_path = runner.adapter.Relation.create_from(
            runner.config, compiled_node
        ).render()
        query = f"SELECT timestamp FROM (DESCRIBE HISTORY {relation_path} LIMIT 1)"
        log_info(
            f"Using Databricks DESCRIBE HISTORY fallback for {compiled_node.unique_id}"
        )

        with runner.adapter.connection_named(compiled_node.unique_id, compiled_node):
            runner.adapter.clear_transaction()
            adapter_response, table = runner.adapter.execute(
                sql=query, auto_begin=False, fetch=True
            )

            if table and len(table.rows) > 0:
                timestamp_value = table.rows[0][0]
                max_loaded_at = parse_query_timestamp_cell(timestamp_value)
                return build_source_freshness_result_from_loaded_at(
                    max_loaded_at=max_loaded_at,
                    compiled_node=compiled_node,
                    adapter_response=adapter_response,
                )
            log_warn(f"No history found for {compiled_node.unique_id}, treating as new")
    except Exception as e:
        log_warn(
            f"Databricks DESCRIBE HISTORY fallback failed for {compiled_node.unique_id}: {e}. "
            "Treating as new."
        )
    return None
