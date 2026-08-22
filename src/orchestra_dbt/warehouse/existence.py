from concurrent.futures import as_completed
from enum import Enum
from typing import Any, Collection, Mapping

from ..logger import log_debug, log_info, log_warn
from .registry import try_registered_override

# (database, schema), both normalised, so `DB.SCHEMA` and `db.schema` group together.
SchemaKey = tuple[str, str]


class RelationExistence(str, Enum):
    EXISTS = "EXISTS"
    MISSING = "MISSING"
    UNKNOWN = "UNKNOWN"


def normalise_part(part: str | None) -> str:
    """Case-fold and strip quote characters from one relation component.

    Deliberately case-insensitive, biasing a mismatch toward EXISTS (today's reuse behaviour)
    rather than a spurious rebuild. `BaseAdapter.get_relation` isn't used here because
    `BaseRelation.matches` raises on a case-only mismatch instead of reporting it absent.
    """
    return (part or "").strip('"`[]').strip().lower()


def build_schema_groups(
    adapter: Any, manifest: Any, unique_ids: Collection[str]
) -> tuple[dict[SchemaKey, Any], dict[SchemaKey, dict[str, str]]]:
    """Group nodes by the schema they materialise into.

    Returns `(schema_relations, members)`: a schema key -> schema-level relation to list, and
    a schema key -> `{unique_id: normalised identifier}`. Relations are built via
    `adapter.Relation.create_from`, which applies quoting and snapshot target
    database/schema correctly -- raw manifest fields don't.
    """
    schema_relations: dict[SchemaKey, Any] = {}
    members: dict[SchemaKey, dict[str, str]] = {}

    for unique_id in unique_ids:
        node = manifest.nodes.get(unique_id)
        if node is None:
            log_debug(
                f"{unique_id} is not in the dbt manifest; skipping existence check."
            )
            continue

        try:
            relation = adapter.Relation.create_from(
                quoting=adapter.config, relation_config=node
            )
        except Exception as e:
            log_debug(f"Could not build a relation for {unique_id}: {e}")
            continue

        identifier = normalise_part(relation.identifier)
        if not identifier or not relation.schema:
            continue

        key: SchemaKey = (
            normalise_part(relation.database),
            normalise_part(relation.schema),
        )
        schema_relations.setdefault(key, relation.without_identifier())
        members.setdefault(key, {})[unique_id] = identifier

    return schema_relations, members


def list_schema_identifiers(
    adapter: Any, adapter_type: str, schema_relation: Any
) -> set[str] | None:
    """Normalised identifiers present in one schema, or None if we could not find out.

    One warehouse round trip per schema, regardless of how many nodes live in it.
    """
    try:
        override = try_registered_override(adapter_type, adapter, schema_relation)
        if override is not None:
            return {normalise_part(identifier) for identifier in override}

        relations = adapter.list_relations_without_caching(schema_relation)
        return {normalise_part(relation.identifier) for relation in relations}
    except Exception as e:
        log_warn(
            f"Could not list relations in {schema_relation}: {e}. "
            f"Leaving reuse decisions for that schema unchanged."
        )
        return None


def _list_sequentially(
    adapter: Any, adapter_type: str, schema_relations: Mapping[SchemaKey, Any]
) -> dict[SchemaKey, set[str] | None]:
    return {
        key: list_schema_identifiers(adapter, adapter_type, relation)
        for key, relation in schema_relations.items()
    }


def _list_in_parallel(
    adapter: Any, adapter_type: str, schema_relations: Mapping[SchemaKey, Any]
) -> dict[SchemaKey, set[str] | None]:
    """Fan out across schemas using dbt's executor, so each thread gets its own connection.

    Not `adapter.set_relations_cache`: it calls `future.result()` bare, so one unreadable
    schema would abort the whole batch instead of degrading to UNKNOWN.
    """
    from dbt_common.utils.executor import executor

    listed: dict[SchemaKey, set[str] | None] = {}
    with executor(adapter.config) as pool:
        futures = {
            pool.submit_connected(
                adapter,
                f"orchestra_list_{index}",
                list_schema_identifiers,
                adapter,
                adapter_type,
                relation,
            ): key
            for index, (key, relation) in enumerate(schema_relations.items())
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                listed[key] = future.result()
            except Exception as e:
                log_warn(f"Could not list relations in {key[0]}.{key[1]}: {e}")
                listed[key] = None
    return listed


def check_relations_exist(
    adapter: Any,
    manifest: Any,
    unique_ids: Collection[str],
    parallel: bool = True,
) -> dict[str, RelationExistence]:
    """Map each node to whether its relation is present in the warehouse.

    Never raises. A schema we cannot read yields UNKNOWN for its nodes only.
    """
    adapter_type: str = adapter.type()
    schema_relations, members = build_schema_groups(adapter, manifest, unique_ids)
    if not schema_relations:
        return {}

    log_info(
        f"Checking {sum(len(m) for m in members.values())} reusable node(s) still exist "
        f"across {len(schema_relations)} warehouse schema(s)."
    )

    if parallel and len(schema_relations) > 1:
        try:
            listed = _list_in_parallel(adapter, adapter_type, schema_relations)
        except Exception as e:
            log_debug(f"Falling back to sequential relation listing: {e}")
            listed = _list_sequentially(adapter, adapter_type, schema_relations)
    else:
        listed = _list_sequentially(adapter, adapter_type, schema_relations)

    existence: dict[str, RelationExistence] = {}
    for key, node_identifiers in members.items():
        identifiers = listed.get(key)
        if identifiers is None:
            existence.update(
                {node_id: RelationExistence.UNKNOWN for node_id in node_identifiers}
            )
            continue

        if not identifiers:
            # Could be a genuinely empty schema, or an adapter swallowing an error.
            log_info(
                f"Schema {key[0]}.{key[1]} contains no relations; "
                f"{len(node_identifiers)} node(s) will be rerun."
            )

        for node_id, identifier in node_identifiers.items():
            existence[node_id] = (
                RelationExistence.EXISTS
                if identifier in identifiers
                else RelationExistence.MISSING
            )

    return existence
