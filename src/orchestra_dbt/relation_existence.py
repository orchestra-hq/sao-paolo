import json
import tempfile
from collections.abc import Collection
from pathlib import Path
from time import perf_counter
from typing import Any, cast

from .compatibility import is_dbt_v2, release_connections
from .logger import log_debug, log_info, log_warn
from .models import Freshness, MaterialisationNode, NodeType, ParsedDag
from .target_finder import find_flag_value, find_target_in_args
from .utils import load_json

# Adapters whose relation listing can't be trusted to gate reuse on. dbt-spark swallows
# unrecognised errors and returns [], which would read as "the whole schema is gone".
_UNSUPPORTED_ADAPTERS = frozenset({"spark"})

_CONNECTION_NAME = "orchestra_relation_existence"

_RESULT_MARKER = "ORCHESTRA_RELATIONS_EXIST="


class _ExistenceCheckFailed(Exception):
    pass


def _warn_unlistable(database: str | None, schema: str, reason: object) -> None:
    log_warn(
        f"Could not list relations in {database}.{schema}: {reason}. "
        f"Leaving reuse decisions for that schema unchanged."
    )


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


def _list_schema(adapter: Any, database: str | None, schema: str) -> set[str] | None:
    """Identifiers in one schema, or None if we could not read it.

    `list_relations` reads dbt's relation cache, which the preceding source-freshness run
    has usually already filled -- so this is normally free, and queries only when cold.
    """
    try:
        return {
            relation.identifier
            for relation in adapter.list_relations(database, schema)
            if relation.identifier
        }
    except Exception as e:
        _warn_unlistable(database, schema, e)
        return None


def find_missing_relations(
    adapter: Any, manifest: Any, candidates: Collection[str]
) -> set[str]:
    """Candidates with no relation in the warehouse; a schema we can't read yields none.

    Casing and quoting are left to the adapter: `_make_match_kwargs` renders the name dbt
    would search for, and listed relations are compared exactly -- the branch
    `BaseRelation._is_exactish_match` takes for anything dbt did not create itself.
    """
    # Keyed on dbt's own schema relation (`without_identifier()`), which is what dbt uses
    # for its cache schemas: equal for two models in the same schema.
    listed: dict[Any, set[str] | None] = {}
    missing: set[str] = set()

    for unique_id in candidates:
        node = manifest.nodes.get(unique_id)
        if node is None:
            log_debug(
                f"{unique_id} is not in the dbt manifest; skipping existence check."
            )
            continue

        try:
            # `create_from` applies quoting and resolves snapshot target database/schema.
            relation = adapter.Relation.create_from(
                quoting=adapter.config, relation_config=node
            )
            search = adapter._make_match_kwargs(
                relation.database, relation.schema, relation.identifier
            )
        except Exception as e:
            log_debug(f"Could not build a relation for {unique_id}: {e}")
            continue

        identifier = search.get("identifier")
        if not identifier or not relation.schema:
            continue

        key = relation.without_identifier()
        if key not in listed:
            listed[key] = _list_schema(adapter, relation.database, relation.schema)

        identifiers = listed[key]
        if identifiers is not None and identifier not in identifiers:
            missing.add(unique_id)

    for schema_relation, identifiers in listed.items():
        if identifiers is not None and not identifiers:
            # Either a genuinely empty schema, or an adapter swallowing an error.
            log_info(f"Schema {schema_relation} contains no relations.")

    return missing


def _relations_exist(
    runner: Any, user_args: list[str], checks: dict[str, list[str]]
) -> dict[str, bool]:
    """`{unique_id: exists}` via one dbt 2.x `run-operation`."""
    # adapter.get_relation lists each schema once and matches names as dbt does.
    sql = (
        "{% set out = {} %}"
        f"{{% for unique_id, r in {json.dumps(checks)}.items() %}}"
        "{% do out.update({unique_id: adapter.get_relation("
        "database=r[0], schema=r[1], identifier=r[2]) is not none}) %}"
        "{% endfor %}"
        f"{{{{ log('{_RESULT_MARKER}' ~ tojson(out), info=True) }}}}"
    )
    # Resolve the same profile as the user's run.
    profile_flags: list[str] = []
    if target := find_target_in_args(user_args):
        profile_flags += ["--target", target]
    for flag in ("--profiles-dir", "--profile", "--vars"):
        if (value := find_flag_value(user_args, flag)) is not None:
            profile_flags += [flag, value]
    with tempfile.TemporaryDirectory() as log_dir:
        result = runner.invoke(
            [
                "run-operation",
                "--sql",
                sql,
                *profile_flags,
                "--log-format-file",
                "json",
                "--log-path",
                log_dir,
                "-q",
            ]
        )
        if not result.success:
            raise _ExistenceCheckFailed(result.exception)
        # run-operation returns nothing to Python; the macro logs its answer instead.
        # Match the message, not the line: the logged command line holds the SQL too.
        with open(Path(log_dir) / "dbt.log") as log_file:
            msg = next(
                (
                    msg
                    for line in log_file
                    if (msg := json.loads(line)["data"].get("msg", "")).startswith(
                        _RESULT_MARKER
                    )
                ),
                None,
            )
    if msg is None:
        raise _ExistenceCheckFailed("no existence result in dbt's log")
    return json.loads(msg.removeprefix(_RESULT_MARKER))


def find_missing_relations_v2(
    candidates: Collection[str], user_args: list[str]
) -> set[str] | None:
    """dbt 2.x has no `dbt.adapters`; on failure, retry per schema to isolate it.
    None for an adapter it is not enabled for."""
    from dbt.cli.main import dbtRunner

    manifest = load_json("target/manifest.json")
    adapter_type = manifest["metadata"]["adapter_type"]
    if adapter_type in _UNSUPPORTED_ADAPTERS:
        log_debug(f"Existence checks are not enabled for the '{adapter_type}' adapter.")
        return None
    manifest_nodes = manifest["nodes"]
    by_schema: dict[tuple[str, str], dict[str, list[str]]] = {}
    for unique_id in candidates:
        node = manifest_nodes[unique_id]
        by_schema.setdefault((node["database"], node["schema"]), {})[unique_id] = [
            node["database"],
            node["schema"],
            node["alias"],
        ]

    runner = dbtRunner()
    try:
        exists = _relations_exist(
            runner,
            user_args,
            {k: v for checks in by_schema.values() for k, v in checks.items()},
        )
    except _ExistenceCheckFailed as e:
        log_debug(f"Relation existence run-operation failed: {e}")
        exists = {}
        failures = 0
        for (database, schema), checks in by_schema.items():
            try:
                exists.update(_relations_exist(runner, user_args, checks))
            except _ExistenceCheckFailed as e:
                _warn_unlistable(database, schema, e)
                failures += 1
                # ponytail: the first two retries failing reads as systemic (profile,
                # auth), not one bad schema; stop rather than retry every schema.
                if failures == 2 and not exists:
                    log_warn("Skipping the existence check for the remaining schemas.")
                    break

    return {unique_id for unique_id, found in exists.items() if not found}


def _find_missing_relations_v1(candidates: Collection[str]) -> set[str] | None:
    """The in-process 1.x check; None for an adapter it is not enabled for."""
    adapter, manifest = _acquire_adapter()
    adapter_type: str = adapter.type()
    if adapter_type in _UNSUPPORTED_ADAPTERS:
        log_debug(f"Existence checks are not enabled for the '{adapter_type}' adapter.")
        return None
    try:
        with adapter.connection_named(_CONNECTION_NAME):
            adapter.clear_transaction()
            return find_missing_relations(adapter, manifest, candidates)
    finally:
        # The real dbt run is a subprocess started straight after this.
        release_connections(adapter)


def apply_relation_existence_gate(
    parsed_dag: ParsedDag,
    paths_to_run: list[str] | None,
    user_args: list[str] | None = None,
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

    started_at = perf_counter()
    try:
        if is_dbt_v2():
            missing = find_missing_relations_v2(candidates, user_args or [])
        else:
            missing = _find_missing_relations_v1(candidates)
        if missing is None:
            return
    except Exception as e:
        log_warn(
            f"Warehouse existence check failed; reuse decisions are unchanged. {e}"
        )
        return

    log_info(
        f"Warehouse existence check for {len(candidates)} node(s) took "
        f"{perf_counter() - started_at:.2f}s."
    )

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
