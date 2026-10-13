from .config import load_orchestra_dbt_settings
from .models import StateApiModel


def apply_integration_account_filter(state: StateApiModel) -> None:
    """Drop entries belonging to other integration accounts.

    Local runs key state by bare node ID (no account prefix), so there is
    nothing to filter on.
    """
    settings = load_orchestra_dbt_settings()
    integration_account_id = settings.integration_account_id
    if integration_account_id and not settings.local_run:
        for key in list(state.state):
            if not key.startswith(integration_account_id):
                state.state.pop(key)
