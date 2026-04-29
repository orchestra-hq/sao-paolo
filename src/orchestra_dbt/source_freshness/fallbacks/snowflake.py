from typing import Any

from ...logger import log_info, log_warn
from .common import (
    build_source_freshness_result_from_loaded_at,
    parse_query_timestamp_cell,
)


def _quote_policy_enabled(relation: Any, component: str) -> bool:
    quote_policy = getattr(relation, "quote_policy", None)
    if isinstance(quote_policy, dict):
        return bool(quote_policy.get(component))
    return bool(getattr(quote_policy, component, False))


def _snowflake_metadata_name(relation: Any, component: str) -> str:
    value = getattr(relation, component, None)
    if value is None:
        raise ValueError(f"Unable to determine relation {component}")

    value_str = str(value)
    if _quote_policy_enabled(relation, component):
        return value_str
    return value_str.upper()


def _snowflake_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _snowflake_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _snowflake_database_reference(relation: Any) -> str:
    database = _snowflake_metadata_name(relation, "database")
    if _quote_policy_enabled(relation, "database"):
        return _snowflake_identifier(database)
    print("database", database)
    return database


def try_snowflake_fallback(runner: Any, compiled_node: Any, _manifest: Any) -> Any:
    try:
        relation = runner.adapter.Relation.create_from(runner.config, compiled_node)
        database = _snowflake_database_reference(relation)
        schema = _snowflake_metadata_name(relation, "schema")
        identifier = _snowflake_metadata_name(relation, "identifier")

        query = (
            "SELECT LAST_ALTERED "
            f"FROM {database}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = {_snowflake_string_literal(schema)} "
            f"AND TABLE_NAME = {_snowflake_string_literal(identifier)} "
            "LIMIT 1"
        )
        log_info(f"Using Snowflake LAST_ALTERED fallback for {compiled_node.unique_id}")

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
            log_warn(
                f"No Snowflake LAST_ALTERED metadata found for {compiled_node.unique_id}, "
                "treating as new"
            )
    except Exception as e:
        log_warn(
            f"Snowflake LAST_ALTERED fallback failed for {compiled_node.unique_id}: {e}. "
            "Treating as new."
        )
    return None
