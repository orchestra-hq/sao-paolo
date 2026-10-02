from datetime import datetime
from functools import lru_cache
from typing import cast

from .logger import log_warn
from .state_backends import resolved_state_backend
from .state_backends.base import StateBackend
from .state_backends.http import HttpStateBackend
from .state_errors import StateLoadError, StateSaveError

__all__ = [
    "StateLoadError",
    "StateSaveError",
    "get_last_updated_from_run_results",
    "load_state",
    "save_state",
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
from .target_finder import artifact_path
from .utils import load_json


def load_state() -> StateApiModel:
    return resolved_state_backend().load()


def save_state(state: StateApiModel, updated_asset_external_ids: set[str]) -> None:
    """Save only this run's updated nodes, leaving every other stored node as it is.

    The HTTP backend upserts each node it is sent, so it is sent only the updates.
    File backends rewrite the whole file, so the updates are merged onto state
    re-read at save time, or a concurrent run's writes would be reverted.
    """
    updates = {
        asset_external_id: state.state[asset_external_id]
        for asset_external_id in updated_asset_external_ids
        if asset_external_id in state.state
    }
    if not updates:
        return
    backend: StateBackend = resolved_state_backend()
    if isinstance(backend, HttpStateBackend):
        backend.save(StateApiModel(state=updates))
        return
    try:
        latest = backend.load()
    except StateLoadError as e:
        raise StateSaveError(
            f"Refusing to save: could not load latest state to merge onto: {e}"
        )
    latest.state.update(updates)
    backend.save(latest)


@lru_cache
def _load_run_results() -> dict:
    try:
        return load_json(path=artifact_path("run_results.json"))
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
            if edge.to_ == node_id and edge.from_ in parsed_dag.nodes:
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
