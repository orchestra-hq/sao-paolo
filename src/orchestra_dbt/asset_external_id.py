import os

from .logger import log_warn


def generate_asset_external_id(
    node_id: str, relation_name: str | None, integration_account_id: str | None
) -> str:
    """
    Used with Orchestra only to differentiate between assets with the same name.

    This environment variable is set in Orchestra. It is ignored when running locally.
    """
    if (
        not integration_account_id
        or os.getenv("ORCHESTRA_LOCAL_RUN", "false").lower() == "true"
    ):
        return node_id

    if not relation_name:
        log_warn(
            f"No relation name found for node '{node_id}'. Using node ID as the asset external ID."
        )
        return f"{integration_account_id}.{node_id}"

    return f"{integration_account_id}.{relation_name}"
