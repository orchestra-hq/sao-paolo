import json
import os

import httpx
from pydantic import ValidationError

from .models import StateApiModel
from .utils import get_base_api_url, get_headers, log_error, log_warn


def load_state() -> StateApiModel:
    try:
        response = httpx.get(
            headers={
                **get_headers(),
                "Accept": "application/json",
            },
            url=f"{get_base_api_url()}/state/{os.environ['ORCHESTRA_DBT_CACHE_KEY']}",
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


def save_state(state: StateApiModel) -> None:
    try:
        response = httpx.patch(
            headers={
                **get_headers(),
                "Content-Type": "application/json",
            },
            json=json.loads(state.model_dump_json(exclude_none=True)),
            url=f"{get_base_api_url()}/state/{os.environ['ORCHESTRA_DBT_CACHE_KEY']}",
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        log_error(f"Failed to save state: {e}")
