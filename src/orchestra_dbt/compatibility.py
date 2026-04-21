SUPPORTED_DBT_CORE_SPEC = ">=1.10,<1.12"


def dbt_core_import_error_message(exc: BaseException) -> str:
    """User-facing text when dbt-core modules cannot be imported."""
    return (
        f"dbt-core is required (supported versions {SUPPORTED_DBT_CORE_SPEC}). "
        f"Install it per README. Import error: {exc}"
    )
