from .models import StateApiModel


def apply_integration_account_filter(
    state: StateApiModel, integration_account_id: str | None
) -> None:
    if integration_account_id:
        for key in list(state.state):
            if not key.startswith(integration_account_id):
                state.state.pop(key)
