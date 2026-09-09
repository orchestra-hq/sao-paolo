from typing import Any

from ..logger import log_warn

_VIEW_RELATION_TYPES = {"view", "materialized_view"}


def is_view(adapter: Any, config: Any, node: Any) -> bool:
    """Whether a source's underlying relation is a view (materialized or not).

    Adapter-agnostic: `adapter.get_relation()` is generic dbt API (no
    per-warehouse SQL) and goes through dbt's relation cache, so this costs
    at most one schema listing per schema per run. Returns False -- rather
    than raising -- when the relation can't be found or the adapter call
    fails, so a lookup problem falls back to trusting the timestamp instead
    of blocking freshness altogether.
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
