from typing import Any, Callable

# Adapters whose relation listing is too expensive or unreliable to gate reuse on. dbt-spark
# swallows unrecognised errors and returns [], which we'd read as "the whole schema is gone".
EXISTENCE_CHECK_UNSUPPORTED: frozenset[str] = frozenset({"spark"})

# Per-adapter override: (adapter, schema_relation) -> normalised identifiers in that schema,
# or None to fall through to dbt's own `list_relations_without_caching`. Empty by default.
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
