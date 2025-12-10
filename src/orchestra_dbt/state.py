import json
import os

import httpx
from pydantic import ValidationError

from .logger import log_error, log_info, log_warn
from .models import StateApiModel


def _get_base_api_url() -> str:
    return f"https://{os.getenv('ORCHESTRA_ENV', 'app').lower()}.getorchestra.io/api/engine/public"


def _get_headers() -> dict:
    return {
        "Authorization": f"Bearer {os.getenv('ORCHESTRA_API_KEY')}",
    }


def load_state() -> StateApiModel:
    try:
        response = httpx.get(
            headers={
                **_get_headers(),
                "Accept": "application/json",
            },
            url=f"{_get_base_api_url()}/state/DBT_CORE",
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        log_warn(f"Failed to load state ({e.response.status_code}): {e.response.text}")
        return StateApiModel(state={})

    try:
        state = StateApiModel.model_validate(response.json())
        log_info("State loaded")
        return state
    except (ValidationError, ValueError) as e:
        log_error(f"Failed to validate state: {e}")
        return StateApiModel(state={})


def save_state(state: StateApiModel) -> None:
    try:
        response = httpx.patch(
            headers={
                **_get_headers(),
                "Content-Type": "application/json",
            },
            json=json.loads(state.model_dump_json(exclude_none=True)),
            url=f"{_get_base_api_url()}/state/DBT_CORE",
        )
        response.raise_for_status()
        log_info("State saved")
    except httpx.HTTPStatusError as e:
        log_warn(f"Failed to save state ({e.response.status_code}): {e.response.text}")
