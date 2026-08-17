from typing import cast

from ..logger import log_debug, log_info, log_warn
from ..models import Freshness, MaterialisationNode, NodeType, ParsedDag
from .adapter_session import (
    AdapterUnavailable,
    acquire_in_process_adapter,
    adapter_connection,
)
from .existence import RelationExistence, check_relations_exist
from .gate import mark_missing_relations_dirty
from .registry import existence_check_supported

__all__ = [
    "AdapterUnavailable",
    "RelationExistence",
    "acquire_in_process_adapter",
    "adapter_connection",
    "apply_relation_existence_gate",
    "check_relations_exist",
    "mark_missing_relations_dirty",
]


def collect_reuse_candidates(
    parsed_dag: ParsedDag, paths_to_run: list[str] | None
) -> list[str]:
    """Nodes we are about to skip and could therefore be wrong about.

    Dirty nodes are already being rebuilt, and nodes without a relation name (dbt leaves it
    unset for ephemeral models) have nothing to look for, so neither is worth a query.
    """
    candidates: list[str] = []
    for node_id, node in parsed_dag.nodes.items():
        if node.node_type != NodeType.MATERIALISATION:
            continue
        materialisation_node = cast(MaterialisationNode, node)
        if materialisation_node.freshness != Freshness.CLEAN:
            continue
        if not materialisation_node.relation_name:
            continue
        if paths_to_run and materialisation_node.dbt_path not in paths_to_run:
            continue
        candidates.append(node_id)
    return candidates


def apply_relation_existence_gate(
    parsed_dag: ParsedDag, paths_to_run: list[str] | None
) -> None:
    """Stop reusing nodes whose warehouse relation no longer exists.

    Mutates `parsed_dag` in place and never raises: if we cannot read the warehouse we leave
    reuse decisions exactly as they were, rather than turning a transient metadata failure
    into a surprise full rebuild.
    """
    candidates = collect_reuse_candidates(parsed_dag, paths_to_run)
    if not candidates:
        log_debug(
            "No reusable nodes to verify; skipping the warehouse existence check."
        )
        return

    try:
        adapter, manifest = acquire_in_process_adapter()
    except AdapterUnavailable as e:
        log_warn(
            f"Skipping the warehouse existence check; reuse decisions are unchanged. {e}"
        )
        return

    adapter_type: str = adapter.type()
    if not existence_check_supported(adapter_type):
        log_debug(
            f"Warehouse existence checks are not enabled for the '{adapter_type}' adapter."
        )
        return

    try:
        with adapter_connection(adapter):
            existence = check_relations_exist(adapter, manifest, candidates)
    except Exception as e:
        log_warn(
            f"Warehouse existence check failed; reuse decisions are unchanged. {e}"
        )
        return

    flipped = mark_missing_relations_dirty(parsed_dag, existence)
    if flipped:
        log_info(f"{flipped} node(s) will be rerun because their relation is missing.")
    else:
        log_debug("Every reusable node still exists in the warehouse.")
