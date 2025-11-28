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
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        log_warn(f"Failed to load state: {e}")
        return StateApiModel(state={})

    try:
        return StateApiModel.model_validate(response.json())
    except (ValidationError, ValueError) as e:
        log_error(f"Failed to validate state: {e}")
        return StateApiModel(state={})


def save_state(
    state: StateApiModel, state_id: str = os.getenv("ORCHESTRA_DBT_CACHE_KEY")
) -> None:
    state_json = state.model_dump_json(exclude_none=True)
    print(state_json)
    try:
        response = httpx.patch(
            headers=HEADERS,
            json=state_json,
            url=f"{BASE_API_URL}/state/{state_id}",
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        print(response.json())
        log_error(f"Failed to save state: {e}")
