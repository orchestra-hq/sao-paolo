from typing import Any, Callable

# Adapters whose relation listing is too expensive or too unreliable to gate reuse on.
#
# dbt-spark swallows unrecognised errors and returns an empty list, which we would read as
# "the whole schema is gone" and rebuild everything. Its Iceberg-v2 fallback also degrades to
# one `describe extended` per table, so the check stops being one query per schema.
EXISTENCE_CHECK_UNSUPPORTED: frozenset[str] = frozenset({"spark"})

# Per-adapter overrides for listing the identifiers present in one schema.
#
# A handler receives (adapter, schema_relation) and returns the set of *normalised* identifiers
# in that schema, or None to fall through to dbt's own `list_relations_without_caching`. Empty
# by default: dbt's per-adapter implementation is the primary path, and this exists so a
# warehouse whose listing turns out slow or wrong can be swapped without touching call sites.
EXISTENCE_OVERRIDE_BY_ADAPTER_TYPE: dict[
    str, Callable[[Any, Any], set[str] | None]
] = {}


def existence_check_supported(adapter_type: str) -> bool:
    return adapter_type not in EXISTENCE_CHECK_UNSUPPORTED


def try_registered_override(
    adapter_type: str, adapter: Any, schema_relation: Any
) -> set[str] | None:
    handler = EXISTENCE_OVERRIDE_BY_ADAPTER_TYPE.get(adapter_type)
    if handler is None:
        return None
    return handler(adapter, schema_relation)
