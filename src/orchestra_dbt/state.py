import json
import os
from datetime import datetime
from functools import lru_cache
from typing import cast

import httpx
from pydantic import ValidationError

from .logger import log_error, log_info, log_warn
from .models import (
    MaterialisationNode,
    NodeType,
    ParsedDag,
    SourceFreshness,
    StateApiModel,
    StateItem,
)
from .utils import get_integration_account_id_from_env, load_json


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
        if integration_account_id := get_integration_account_id_from_env():
            for key in list(state.state):
                if not key.startswith(integration_account_id):
                    state.state.pop(key)
        log_info(
            f"State loaded. Retrieved {len(state.state)} items.",
        )
        return state
    except (ValidationError, ValueError) as e:
        log_error(f"Failed to validate state: {e}")
        return StateApiModel(state={})


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

        asset_external_id = node_id
        if integration_account_id := get_integration_account_id_from_env():
            asset_external_id = f"{integration_account_id}.{node_id}"

        state.state[asset_external_id] = StateItem(
            checksum=materialisation_node.checksum,
            last_updated=last_updated_from_run_results,
            sources=sources_dict,
        )


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
