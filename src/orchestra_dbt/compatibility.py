from importlib.metadata import PackageNotFoundError, version
from typing import Any

from .constants import SUPPORTED_DBT_CORE_SPEC


def dbt_core_import_error_message(exc: BaseException) -> str:
    return (
        f"dbt-core is required (supported versions {SUPPORTED_DBT_CORE_SPEC}). "
        f"Install it per README. Import error: {exc}"
    )


def is_dbt_v2() -> bool:
    """dbt 1.x installs as `dbt-core`; v2 as `dbt` or `dbt-oss`."""
    for distribution in ("dbt-core", "dbt", "dbt-oss"):
        try:
            return int(version(distribution).split(".")[0]) >= 2
        except PackageNotFoundError:
            continue
    return False


def release_connections(adapter: Any) -> None:
    """Close an in-process adapter's connections before dbt runs as a subprocess."""
    adapter.cleanup_connections()
    if adapter.type() == "duckdb":
        # dbt-duckdb holds its database (and file lock) process-wide until this.
        adapter.connections.close_all_connections()
