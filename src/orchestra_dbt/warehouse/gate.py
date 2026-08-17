from typing import Mapping, cast

from ..logger import log_info
from ..models import Freshness, MaterialisationNode, NodeType, ParsedDag
from .existence import RelationExistence


def mark_missing_relations_dirty(
    parsed_dag: ParsedDag, existence: Mapping[str, RelationExistence]
) -> int:
    """Force nodes whose warehouse relation has gone missing back into the run.

    Only CLEAN nodes reported MISSING are flipped; EXISTS and UNKNOWN leave the DAG alone.
    Because this runs before `calculate_nodes_to_run`, the usual topological sweep then
    propagates the new DIRTY state downstream for free.

    Returns the number of nodes flipped.
    """
    flipped = 0
    for node_id, status in existence.items():
        if status != RelationExistence.MISSING:
            continue

        node = parsed_dag.nodes.get(node_id)
        if node is None or node.node_type != NodeType.MATERIALISATION:
            continue

        materialisation_node = cast(MaterialisationNode, node)
        if materialisation_node.freshness != Freshness.CLEAN:
            continue

        relation = materialisation_node.relation_name or node_id
        materialisation_node.freshness = Freshness.DIRTY
        materialisation_node.reason = f"Relation {relation} no longer exists in the warehouse - deleted hence rerun."
        log_info(f"{node_id} was deleted from the warehouse hence rerun.")
        flipped += 1

    return flipped
