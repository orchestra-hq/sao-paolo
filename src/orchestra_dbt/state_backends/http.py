import json

import httpx
from pydantic import ValidationError

from ..config import get_orchestra_api_key, load_orchestra_dbt_settings
from ..logger import log_error, log_warn
from ..models import StateApiModel
from ..state_filters import apply_integration_account_filter
from .logging import log_state_loaded, log_state_saved


class HttpStateBackend:
    def _base_api_url(self) -> str:
        env_name = load_orchestra_dbt_settings().orchestra_env
        return f"https://{env_name}.getorchestra.io/api/engine/public"

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {get_orchestra_api_key()}",
        }

    def load(self) -> StateApiModel:
        try:
            response = httpx.get(
                headers={
                    **self._headers(),
                    "Accept": "application/json",
                },
                url=f"{self._base_api_url()}/state/DBT_CORE",
                timeout=httpx.Timeout(timeout=30),
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            log_warn(
                f"Failed to load state ({e.response.status_code}): {e.response.text}"
            )
            return StateApiModel(state={})
        except httpx.RequestError as e:
            log_warn(f"Failed to load state due to network error: {e}")
            return StateApiModel(state={})

        try:
            state = StateApiModel.model_validate(response.json())
            apply_integration_account_filter(state)
            log_state_loaded("http", state)
            return state
        except (ValidationError, ValueError) as e:
            log_error(f"Failed to validate state: {e}")
            return StateApiModel(state={})

    def save(self, state: StateApiModel) -> None:
        try:
            response = httpx.patch(
                headers={
                    **self._headers(),
                    "Content-Type": "application/json",
                },
                json=json.loads(state.model_dump_json(exclude_none=True)),
                url=f"{self._base_api_url()}/state/DBT_CORE",
                timeout=httpx.Timeout(timeout=30),
            )
            response.raise_for_status()
            log_state_saved("http")
        except httpx.HTTPStatusError as e:
            log_warn(
                f"Failed to save state ({e.response.status_code}): {e.response.text}"
            )
        except httpx.RequestError as e:
            log_warn(f"Failed to save state due to network error: {e}")
