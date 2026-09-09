from typing import Any

from ..logger import log_warn
from ..models import RelationType


def get_relation_type(adapter: Any, config: Any, node: Any) -> RelationType | None:
    """Look up a source's relation kind in the warehouse.

    Adapter-agnostic: `adapter.get_relation()` is generic `BaseAdapter` API
    backed by each adapter's own `list_relations_without_caching` macro, so
    no per-warehouse SQL is needed here. Results go through dbt's relation
    cache, so this costs at most one lightweight "list the relations in this
    schema" query per schema per run, and nothing for later sources in a
    schema already listed.

    Returns None when the kind can't be determined, which leaves the caller
    on its default behaviour rather than acting on a guess.
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
