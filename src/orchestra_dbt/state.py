from datetime import datetime
from functools import lru_cache
from typing import cast

from .state_backends import resolved_state_backend
from .state_errors import StateLoadError, StateSaveError
from .logger import log_warn

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
from .utils import load_json


def load_state() -> StateApiModel:
    return resolved_state_backend().load()


def save_state(state: StateApiModel) -> None:
    resolved_state_backend().save(state)


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
