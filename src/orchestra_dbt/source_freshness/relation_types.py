from typing import Any

from ..logger import log_warn
from ..models import RelationType


def get_relation_type(adapter: Any, config: Any, node: Any) -> RelationType | None:
    """Look up a source's relation kind, or None if it can't be determined.

    `adapter.get_relation()` is generic dbt API (no per-warehouse SQL) and
    goes through dbt's relation cache, so this costs at most one schema
    listing per schema per run.
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
        return None

    if found is None:
        return None
    return RelationType.parse(getattr(found, "type", None))
