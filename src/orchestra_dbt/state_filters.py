from .config import get_integration_account_id
from .models import StateApiModel


def apply_integration_account_filter(state: StateApiModel) -> None:
    if integration_account_id := get_integration_account_id():
        for key in list(state.state):
            if not key.startswith(integration_account_id):
                state.state.pop(key)
