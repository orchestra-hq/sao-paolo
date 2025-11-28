import os
import httpx

from pydantic import ValidationError

from .models import StateApiModel
from .utils import BASE_API_URL, HEADERS, log_warn, log_error


def load_state(state_id: str = os.getenv("ORCHESTRA_DBT_CACHE_KEY")) -> StateApiModel:
    try:
        response = httpx.get(
            headers=HEADERS,
            url=f"{BASE_API_URL}/state/{state_id}",
        )
    except httpx.HTTPStatusError as e:
        log_warn(f"Failed to load state: {e}")
        return StateApiModel(state={})

    try:
        return StateApiModel.model_validate_json(response.json())
    except (ValidationError, ValueError) as e:
        log_error(f"Failed to validate state: {e}")
        return StateApiModel(state={})


def save_state(state_id: str, state: StateApiModel) -> None:
    try:
        httpx.patch(
            headers=HEADERS,
            json=state.model_dump(exclude_none=True),
            url=f"{BASE_API_URL}/state/{state_id}",
        )
    except httpx.HTTPStatusError as e:
        log_error(f"Failed to save state: {e}")
