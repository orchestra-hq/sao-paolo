from typing import Any, Collection, cast

from .logger import log_debug, log_info, log_warn
from .models import Freshness, MaterialisationNode, NodeType, ParsedDag

# Adapters whose relation listing can't be trusted to gate reuse on. dbt-spark swallows
# unrecognised errors and returns [], which would read as "the whole schema is gone".
_UNSUPPORTED_ADAPTERS = frozenset({"spark"})

_CONNECTION_NAME = "orchestra_relation_existence"


def _normalise(part: str | None, fold_case: bool = True) -> str:
    """Strip quotes from a relation component, folding case only where that is safe.

    Mirrors `BaseAdapter._make_match_kwargs`: unquoted components take the warehouse's own
    casing so both sides need folding; quoted ones keep theirs, and on BigQuery `Foo` and
    `foo` really are different tables. Whitespace is never stripped -- a quoted identifier
    may legitimately carry leading or trailing spaces, and ` x ` is not the table `x`.
    """
    part = (part or "").strip('"`[]')
    return part.lower() if fold_case else part


def _fold_flags(relation: Any) -> tuple[bool, bool, bool]:
    """Whether (database, schema, identifier) may be case-folded, per dbt's own answer.

    `Relation.create_from` has already merged the adapter's default quote policy, the
    project `quoting:` config and any per-node override into `quote_policy` -- so read that
    rather than deciding ourselves. Unreadable policy folds, biasing toward "exists" and so
    toward leaving reuse decisions alone.
    """
    try:
        policy = relation.quote_policy
        return (
            policy.database is False,
            policy.schema is False,
            policy.identifier is False,
        )
    except Exception:
        return (True, True, True)


def _acquire_adapter() -> tuple[Any, Any]:
    """Return the (adapter, manifest) dbt already registered in this process.

    The preceding in-process `dbt source freshness` sets both up via `@requires.manifest`,
    and `reset_adapters()` on each invoke leaves only that one -- hence expecting exactly one.
    """
    from dbt.adapters.factory import FACTORY, get_adapter_by_type

    registered = list(FACTORY.adapters)
    if len(registered) != 1:
        raise RuntimeError(
            f"Expected exactly one registered dbt adapter, found {registered or 'none'}."
        )

    adapter = get_adapter_by_type(registered[0])
    manifest = adapter.get_macro_resolver()
    if manifest is None or not getattr(manifest, "nodes", None):
        raise RuntimeError("The registered dbt adapter has no manifest attached.")

    return adapter, manifest


def collect_reuse_candidates(
    parsed_dag: ParsedDag, paths_to_run: list[str] | None
) -> dict[str, MaterialisationNode]:
    """Nodes we're about to skip and could be wrong about (dirty and ephemeral don't apply)."""
    candidates: dict[str, MaterialisationNode] = {}
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
        candidates[node_id] = materialisation_node
    return candidates


def _list_schema(
    adapter: Any, database: str | None, schema: str, fold_identifier: bool
) -> set[str] | None:
    """Normalised identifiers in one schema, or None if we could not read it.

    `list_relations` reads dbt's relation cache, which the preceding source-freshness run
    has usually already filled -- so this is normally free, and queries only when cold.
    """
    try:
        return {
            _normalise(relation.identifier, fold_identifier)
            for relation in adapter.list_relations(database, schema)
        }
    except Exception as e:
        log_warn(
            f"Could not list relations in {database}.{schema}: {e}. "
            f"Leaving reuse decisions for that schema unchanged."
        )
        return None


def find_missing_relations(
    adapter: Any, manifest: Any, candidates: Collection[str]
) -> set[str]:
    """Candidates with no relation in the warehouse; a schema we can't read yields none.

    `create_from` is used over raw manifest fields: it applies quoting and resolves snapshot
    target database/schema.
    """
    # Keyed on the fold flags too, so a node whose quoting differs gets its own listing.
    listed: dict[tuple[str, str, bool], set[str] | None] = {}
    missing: set[str] = set()

    for unique_id in candidates:
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

        fold_database, fold_schema, fold_identifier = _fold_flags(relation)
        identifier = _normalise(relation.identifier, fold_identifier)
        if not identifier or not relation.schema:
            continue

        key = (
            _normalise(relation.database, fold_database),
            _normalise(relation.schema, fold_schema),
            fold_identifier,
        )
        if key not in listed:
            listed[key] = _list_schema(
                adapter, relation.database, relation.schema, fold_identifier
            )

        identifiers = listed[key]
        if identifiers is not None and identifier not in identifiers:
            missing.add(unique_id)

    for (database, schema, _), identifiers in listed.items():
        if identifiers is not None and not identifiers:
            # Either a genuinely empty schema, or an adapter swallowing an error.
            log_info(f"Schema {database}.{schema} contains no relations.")

    return missing


def apply_relation_existence_gate(
    parsed_dag: ParsedDag, paths_to_run: list[str] | None
) -> None:
    """Stop reusing nodes whose warehouse relation no longer exists.

    Mutates `parsed_dag` in place and never raises -- a failed check leaves reuse decisions
    alone. Runs before `calculate_nodes_to_run` so DIRTY propagates downstream.
    """
    candidates = collect_reuse_candidates(parsed_dag, paths_to_run)
    if not candidates:
        log_debug(
            "No reusable nodes to verify; skipping the warehouse existence check."
        )
        return

    try:
        adapter, manifest = _acquire_adapter()
        adapter_type: str = adapter.type()
        if adapter_type in _UNSUPPORTED_ADAPTERS:
            log_debug(
                f"Existence checks are not enabled for the '{adapter_type}' adapter."
            )
            return
        try:
            with adapter.connection_named(_CONNECTION_NAME):
                adapter.clear_transaction()
                missing = find_missing_relations(adapter, manifest, candidates)
        finally:
            # The real dbt run is a subprocess started straight after this.
            adapter.cleanup_connections()
    except Exception as e:
        log_warn(
            f"Warehouse existence check failed; reuse decisions are unchanged. {e}"
        )
        return

    for unique_id in missing:
        node = candidates[unique_id]
        node.freshness = Freshness.DIRTY
        node.reason = (
            f"Relation {node.relation_name} no longer exists in the warehouse "
            f"- deleted hence rerun."
        )
        log_info(f"{unique_id} was deleted from the warehouse hence rerun.")

    if missing:
        log_info(
            f"{len(missing)} node(s) will be rerun because their relation is missing."
        )
    else:
        log_debug("Every reusable node still exists in the warehouse.")
