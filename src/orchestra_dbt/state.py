from datetime import datetime
from functools import lru_cache
from typing import cast

from .logger import log_warn
from .state_backends import resolved_state_backend
from .state_backends.base import StateBackend
from .state_errors import StateLoadError, StateSaveError

__all__ = [
    "StateLoadError",
    "StateSaveError",
    "get_last_updated_from_run_results",
    "load_state",
    "save_state",
    "save_updated_state",
    "update_state",
]
from .models import (
    MaterialisationNode,
    NodeType,
    ParsedDag,
    SourceFreshness,
    StateApiModel,
    StateItem,
)
from .utils import load_json


def load_state() -> StateApiModel:
    return resolved_state_backend().load()


def save_state(state: StateApiModel) -> None:
    resolved_state_backend().save(state)


def save_updated_state(
    state: StateApiModel, updated_asset_external_ids: set[str]
) -> None:
    """Merge only this run's updated nodes onto the latest stored state.

    Re-reads state at save time so a run with a narrow selector does not revert
    another concurrent run's writes to nodes it did not execute itself.
    """
    backend: StateBackend = resolved_state_backend()
    try:
        latest = backend.load()
    except StateLoadError:
        latest = StateApiModel(state={})
    for asset_external_id in updated_asset_external_ids:
        updated_item = state.state.get(asset_external_id)
        if updated_item is not None:
            latest.state[asset_external_id] = updated_item
    backend.save(latest)


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
) -> set[str]:
    updated_asset_external_ids: set[str] = set()
    for node_id, node in parsed_dag.nodes.items():
        if node.node_type == NodeType.SOURCE:
            continue

        materialisation_node: MaterialisationNode = cast(MaterialisationNode, node)
        last_updated_from_run_results = get_last_updated_from_run_results(node_id)
        if not last_updated_from_run_results:
            continue

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
        updated_asset_external_ids.add(materialisation_node.asset_external_id)

    return updated_asset_external_ids
