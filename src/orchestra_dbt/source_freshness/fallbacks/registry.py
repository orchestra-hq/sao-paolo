from collections.abc import Callable
from typing import Any

from .databricks import try_databricks_fallback
from .snowflake import try_snowflake_fallback

FALLBACK_BY_ADAPTER_TYPE: dict[str, Callable[..., Any]] = {
    "databricks": try_databricks_fallback,
    "snowflake": try_snowflake_fallback,
}


def loaded_at_fields_unset(compiled_node: Any) -> bool:
    return (
        compiled_node.loaded_at_query is None and compiled_node.loaded_at_field is None
    )


def try_registered_fallback(
    adapter_type: str, runner: Any, compiled_node: Any, manifest: Any
) -> Any | None:
    if (handler := FALLBACK_BY_ADAPTER_TYPE.get(adapter_type)) is None:
        return None
    return handler(runner, compiled_node, manifest)
