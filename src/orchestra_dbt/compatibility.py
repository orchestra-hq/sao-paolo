from typing import Any

from .constants import SUPPORTED_DBT_CORE_SPEC


def dbt_core_import_error_message(exc: BaseException) -> str:
    return (
        f"dbt-core is required (supported versions {SUPPORTED_DBT_CORE_SPEC}). "
        f"Install it per README. Import error: {exc}"
    )


def release_connections(adapter: Any) -> None:
    """Close an in-process adapter's connections before dbt runs as a subprocess."""
    adapter.cleanup_connections()
    if adapter.type() == "duckdb":
        # dbt-duckdb holds its database (and file lock) process-wide until this.
        adapter.connections.close_all_connections()
