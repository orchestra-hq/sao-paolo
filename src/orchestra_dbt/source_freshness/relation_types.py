from typing import Any

from ..logger import log_warn

_VIEW_RELATION_TYPES = {"view", "materialized_view"}


def is_view(adapter: Any, config: Any, node: Any) -> bool:
    """Whether a source's relation is a view. Adapter-agnostic (no
    per-warehouse SQL); returns False rather than raising on any failure.
    """
    try:
        relation = adapter.Relation.create_from(config, node)
        found = adapter.get_relation(
            database=relation.database,
            schema=relation.schema,
            identifier=relation.identifier,
        )
    except Exception as e:
        log_warn(f"Could not look up relation type for {node.unique_id}: {e}")
        return False

    if found is None:
        return False
    relation_type = getattr(found, "type", None)
    return str(getattr(relation_type, "value", relation_type)).lower() in (
        _VIEW_RELATION_TYPES
    )
