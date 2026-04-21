from .constants import SUPPORTED_DBT_CORE_SPEC


def dbt_core_import_error_message(exc: BaseException) -> str:
    return (
        f"dbt-core is required (supported versions {SUPPORTED_DBT_CORE_SPEC}). "
        f"Install it per README. Import error: {exc}"
    )
