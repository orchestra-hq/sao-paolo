from .logger import log_warn


def generate_asset_external_id(
    node_id: str,
    relation_name: str | None,
    integration_account_id: str | None,
    local_run: bool = False,
) -> str:
    """
    Generate a stable external ID used for Orchestra state keys.

    When `local_run` is true (from `[tool.orchestra_dbt]` / `ORCHESTRA_LOCAL_RUN`),
    IDs match a plain dbt run and use `node_id` only.
    """
    if not integration_account_id or local_run:
        return node_id

    if not relation_name:
        log_warn(
            f"No relation name found for node '{node_id}'. Using node ID fallback for the asset external ID."
        )
        return f"{integration_account_id}.{node_id}"

    return f"{integration_account_id}.{relation_name}"
