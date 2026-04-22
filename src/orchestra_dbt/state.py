import json
import os
import tempfile
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import cast

import httpx
from pydantic import ValidationError

from .config import (
    effective_state_file_path,
    get_integration_account_id,
    load_orchestra_dbt_settings,
)
from .logger import log_error, log_info, log_warn
from .models import (
    MaterialisationNode,
    NodeType,
    ParsedDag,
    SourceFreshness,
    StateApiModel,
    StateItem,
)
from .utils import load_json


class StateLoadError(Exception):
    """Raised when state cannot be loaded from the configured backend."""


class StateSaveError(Exception):
    """Raised when state cannot be written to the configured backend."""


def _get_base_api_url() -> str:
    env_name = load_orchestra_dbt_settings().orchestra_env
    return f"https://{env_name}.getorchestra.io/api/engine/public"


def _get_headers() -> dict:
    return {
        "Authorization": f"Bearer {os.getenv('ORCHESTRA_API_KEY')}",
    }


def _apply_integration_account_filter(state: StateApiModel) -> None:
    if integration_account_id := get_integration_account_id():
        for key in list(state.state):
            if not key.startswith(integration_account_id):
                state.state.pop(key)


def _load_state_http() -> StateApiModel:
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
        _apply_integration_account_filter(state)
        log_info(
            f"State loaded. Retrieved {len(state.state)} items.",
        )
        return state
    except (ValidationError, ValueError) as e:
        log_error(f"Failed to validate state: {e}")
        return StateApiModel(state={})


def _load_state_file(path: Path) -> StateApiModel:
    if not path.is_file():
        raise StateLoadError(f"State file not found: {path}")

    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise StateLoadError(f"State file is not valid JSON ({path}): {e}")

    try:
        state = StateApiModel.model_validate(data)
    except (ValidationError, ValueError) as e:
        raise StateLoadError(f"State file failed validation ({path}): {e}")

    _apply_integration_account_filter(state)
    log_info(f"State loaded from file. Retrieved {len(state.state)} items.")
    return state


def load_state() -> StateApiModel:
    path = effective_state_file_path()
    if path is not None:
        return _load_state_file(path)
    return _load_state_http()


@lru_cache
def _load_run_results() -> dict:
    try:
        return load_json(path="target/run_results.json")
    except FileNotFoundError:
        return {}


def get_last_updated_from_run_results(node_id: str) -> datetime | None:
    try:
        for r in _load_run_results().get("results", []):
            if r["unique_id"] == node_id and r["status"] == "success":
                return r["timing"][-1]["completed_at"]
    except Exception as e:
        log_warn(f"Failed to get last updated from run results for '{node_id}': {e}")
    return None


def update_state(
    state: StateApiModel, parsed_dag: ParsedDag, source_freshness: SourceFreshness
) -> None:
    for node_id, node in parsed_dag.nodes.items():
        if node.node_type == NodeType.SOURCE:
            continue

        materialisation_node: MaterialisationNode = cast(MaterialisationNode, node)
        last_updated_from_run_results = get_last_updated_from_run_results(node_id)
        if not last_updated_from_run_results:
            continue

        # Build sources dict from parent nodes that are sources
        sources_dict: dict[str, datetime] = {}
        for edge in parsed_dag.edges:
            if edge.to_ == node_id:
                if edge.from_ in parsed_dag.nodes:
                    parent_node = parsed_dag.nodes[edge.from_]
                    if (
                        parent_node.node_type == NodeType.SOURCE
                        and edge.from_ in source_freshness.sources
                    ):
                        sources_dict[edge.from_] = source_freshness.sources[edge.from_]

        state.state[materialisation_node.asset_external_id] = StateItem(
            checksum=materialisation_node.checksum,
            last_updated=last_updated_from_run_results,
            sources=sources_dict,
        )


def _save_state_http(state: StateApiModel) -> None:
    try:
        response = httpx.patch(
            headers={
                **_get_headers(),
                "Content-Type": "application/json",
            },
            json=json.loads(state.model_dump_json(exclude_none=True)),
            url=f"{_get_base_api_url()}/state/DBT_CORE",
            timeout=httpx.Timeout(timeout=30),
        )
        response.raise_for_status()
        log_info("State saved")
    except httpx.HTTPStatusError as e:
        log_warn(f"Failed to save state ({e.response.status_code}): {e.response.text}")
    except httpx.TimeoutException as e:
        log_warn(f"Failed to save state due to timeout: {e}")


def _save_state_file(path: Path, state: StateApiModel) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload_bytes = state.model_dump_json(exclude_none=True).encode("utf-8")
    fd, tmp_path = tempfile.mkstemp(
        dir=path.parent, prefix=".orchestra_state_", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "wb") as tmp_file:
            tmp_file.write(payload_bytes)
        os.replace(tmp_path, path)
    except OSError as e:
        try:
            if os.path.isfile(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass
        raise StateSaveError(f"Failed to save state file ({path}): {e}") from e
    log_info("State saved")


def save_state(state: StateApiModel) -> None:
    path = effective_state_file_path()
    if path is not None:
        _save_state_file(path, state)
        return
    _save_state_http(state)
