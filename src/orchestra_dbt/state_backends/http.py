import json
import random
import time

import httpx
from pydantic import ValidationError

from ..config import get_orchestra_api_key, load_orchestra_dbt_settings
from ..logger import log_error, log_info, log_warn
from ..models import StateApiModel
from ..state_errors import StateLoadError
from ..state_filters import apply_integration_account_filter
from .logging import log_state_loaded, log_state_saved

_SAVE_ATTEMPTS = 3


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
            raise StateLoadError(
                f"Failed to load state ({e.response.status_code}): {e.response.text}"
            )
        except httpx.RequestError as e:
            log_warn(f"Failed to load state due to network error: {e}")
            raise StateLoadError(f"Failed to load state due to network error: {e}")

        try:
            state = StateApiModel.model_validate(response.json())
            apply_integration_account_filter(state)
            log_state_loaded("http", state)
            return state
        except (ValidationError, ValueError) as e:
            log_error(f"Failed to validate state: {e}")
            raise StateLoadError(f"Failed to validate state: {e}")

    def save(self, state: StateApiModel) -> None:
        for attempt in range(1, _SAVE_ATTEMPTS + 1):
            try:
                self._patch_state(state)
                log_state_saved("http")
                return
            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                if attempt == _SAVE_ATTEMPTS or not _is_transient(status_code):
                    log_warn(f"Failed to save state ({status_code}): {e.response.text}")
                    return
            except httpx.RequestError as e:
                if attempt == _SAVE_ATTEMPTS:
                    log_warn(f"Failed to save state due to network error: {e}")
                    return
            log_info(f"Retrying state save ({attempt + 1}/{_SAVE_ATTEMPTS})")
            time.sleep(2 ** (attempt - 1) + random.uniform(0, 1))

    def _patch_state(self, state: StateApiModel) -> None:
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


def _is_transient(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500
