import re
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import src.orchestra_dbt.dag as dag_module
import src.orchestra_dbt.relation_existence as relation_existence
from src.orchestra_dbt.config import OrchestraDbtSettings
from src.orchestra_dbt.dag import construct_dag
from src.orchestra_dbt.models import (
    Freshness,
    FreshnessConfig,
    MaterialisationNode,
    ParsedDag,
    SourceFreshness,
    SourceNode,
    StateApiModel,
    StateItem,
)
from src.orchestra_dbt.relation_existence import (
    apply_relation_existence_gate,
    collect_reuse_candidates,
    find_missing_relations,
)
from src.orchestra_dbt.sao import calculate_nodes_to_run


class FakeRelation:
    """Stands in for a dbt BaseRelation, including the quote policy `create_from` merges."""

    def __init__(
        self,
        database: str | None,
        schema: str | None,
        identifier: str | None,
        quoted: bool = False,
        policy: tuple[bool, bool, bool] | None = None,
    ):
        self.database = database
        self.schema = schema
        self.identifier = identifier
        self._policy = policy or (quoted, quoted, quoted)
        self.quote_policy = SimpleNamespace(
            database=self._policy[0], schema=self._policy[1], identifier=self._policy[2]
        )

    def without_identifier(self) -> "FakeRelation":
        return FakeRelation(self.database, self.schema, None, policy=self._policy)

    def _key(self) -> tuple:
        return (self.database, self.schema, self.identifier, self._policy)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, FakeRelation) and self._key() == other._key()

    def __hash__(self) -> int:
        return hash(self._key())

    def __repr__(self) -> str:
        return f"{self.database}.{self.schema}"


def make_manifest(nodes: dict[str, tuple[str, str, str]]) -> SimpleNamespace:
    """nodes: {unique_id: (database, schema, alias)}"""
    return SimpleNamespace(
        nodes={
            unique_id: {"database": db, "schema": schema, "alias": alias}
            for unique_id, (db, schema, alias) in nodes.items()
        }
    )


def make_adapter(
    listings: dict[tuple[str, str], list[str] | Exception],
    adapter_type: str = "postgres",
    quoted: bool = False,
    fold: str = "lower",
) -> tuple[MagicMock, list[tuple[str, str]]]:
    """`fold` is the adapter's own casing: the base adapter lowercases, Snowflake uppercases."""
    calls: list[tuple[str, str]] = []
    adapter = MagicMock()
    adapter.type.return_value = adapter_type
    adapter.Relation.create_from.side_effect = lambda quoting, relation_config: (
        FakeRelation(
            relation_config["database"],
            relation_config["schema"],
            relation_config["alias"],
            quoted,
        )
    )

    def make_match_kwargs(database, schema, identifier):
        """Mirrors `BaseAdapter._make_match_kwargs`: fold only unquoted components."""
        parts = {"database": database, "schema": schema, "identifier": identifier}
        if not quoted:
            parts = {
                k: (v.lower() if fold == "lower" else v.upper()) if v else v
                for k, v in parts.items()
            }
        return {k: v for k, v in parts.items() if v is not None}

    adapter._make_match_kwargs.side_effect = make_match_kwargs

    def list_relations(database, schema):
        key = (database, schema)
        calls.append(key)
        outcome = listings[key]
        if isinstance(outcome, Exception):
            raise outcome
        return [FakeRelation(database, schema, i) for i in outcome]

    adapter.list_relations.side_effect = list_relations
    return adapter, calls


def _node(
    node_id: str,
    freshness: Freshness = Freshness.CLEAN,
    relation_name: str | None = "db.schema.model",
    dbt_path: str | None = None,
) -> MaterialisationNode:
    return MaterialisationNode(
        asset_external_id=node_id,
        checksum="abc",
        dbt_path=dbt_path or f"models/{node_id}.sql",
        file_path=dbt_path or f"models/{node_id}.sql",
        freshness_config=FreshnessConfig(),
        freshness=freshness,
        reason="Model in same state as last run.",
        sources={},
        relation_name=relation_name,
    )


class TestFindMissingRelations:
    def test_present_absent_and_case_insensitive(self) -> None:
        """The warehouse reports its own casing; the adapter renders what to look for."""
        manifest = make_manifest(
            {
                "model.p.here": ("db", "analytics", "here"),
                "model.p.cased": ("db", "analytics", "my_model"),
                "model.p.gone": ("db", "analytics", "gone"),
            }
        )
        adapter, _ = make_adapter(
            {("db", "analytics"): ["HERE", "MY_MODEL"]}, fold="upper"
        )

        missing = find_missing_relations(
            adapter, manifest, dict.fromkeys(manifest.nodes)
        )

        assert missing == {"model.p.gone"}

    def test_empty_schema_marks_everything_missing(self) -> None:
        manifest = make_manifest({"model.p.a": ("db", "analytics", "a")})
        adapter, _ = make_adapter({("db", "analytics"): []})

        assert find_missing_relations(
            adapter, manifest, dict.fromkeys(manifest.nodes)
        ) == {"model.p.a"}

    def test_one_listing_per_schema_not_per_node(self) -> None:
        manifest = make_manifest(
            {f"model.p.n{i}": ("db", "analytics", f"n{i}") for i in range(25)}
            | {"model.p.other": ("db", "staging", "other")}
        )
        adapter, calls = make_adapter(
            {
                ("db", "analytics"): [f"n{i}" for i in range(25)],
                ("db", "staging"): ["other"],
            }
        )

        missing = find_missing_relations(
            adapter, manifest, dict.fromkeys(manifest.nodes)
        )

        assert missing == set()
        assert sorted(calls) == [("db", "analytics"), ("db", "staging")]

    def test_unreadable_schema_never_forces_a_rebuild_and_is_isolated(self) -> None:
        """The core safety contract: a schema we cannot read must not look 'missing'."""
        manifest = make_manifest(
            {
                "model.p.broken": ("db", "locked", "broken"),
                "model.p.gone": ("db", "analytics", "gone"),
            }
        )
        adapter, _ = make_adapter(
            {
                ("db", "locked"): Exception("permission denied"),
                ("db", "analytics"): [],
            }
        )

        missing = find_missing_relations(
            adapter, manifest, dict.fromkeys(manifest.nodes)
        )

        assert missing == {"model.p.gone"}

    def test_skips_nodes_absent_from_the_manifest_or_lacking_a_schema(self) -> None:
        manifest = make_manifest(
            {
                "model.p.a": ("db", "analytics", "a"),
                "snapshot.p.s": ("db", "snapshots", "s"),
                "model.p.no_schema": ("db", "", "d"),
            }
        )
        adapter, calls = make_adapter(
            {("db", "analytics"): ["a"], ("db", "snapshots"): ["s"]}
        )

        missing = find_missing_relations(
            adapter, manifest, dict.fromkeys([*manifest.nodes, "model.p.vanished"])
        )

        assert missing == set()
        assert sorted(calls) == [("db", "analytics"), ("db", "snapshots")]


class TestCaseSensitivity:
    """Casing is the adapter's call: `_make_match_kwargs` renders the name dbt searches for."""

    def test_unquoted_adapter_matches_across_case(self) -> None:
        """Snowflake reports `MY_MODEL` for a model dbt configured as `my_model`."""
        manifest = make_manifest({"model.p.a": ("db", "analytics", "my_model")})
        adapter, _ = make_adapter({("db", "analytics"): ["MY_MODEL"]}, fold="upper")

        assert (
            find_missing_relations(adapter, manifest, dict.fromkeys(manifest.nodes))
            == set()
        )

    def test_folding_direction_is_not_assumed(self) -> None:
        """The base adapter lowercases where Snowflake uppercases; we use whichever it gives."""
        manifest = make_manifest({"model.p.a": ("db", "analytics", "My_Model")})
        adapter, _ = make_adapter({("db", "analytics"): ["my_model"]}, fold="lower")

        assert (
            find_missing_relations(adapter, manifest, dict.fromkeys(manifest.nodes))
            == set()
        )

    def test_quoted_adapter_treats_differing_case_as_missing(self) -> None:
        """On BigQuery `Foo` and `foo` are different tables, so folding would report a
        genuinely missing relation as present -- the bug this check exists to catch.
        """
        manifest = make_manifest({"model.p.a": ("db", "analytics", "Foo")})
        adapter, _ = make_adapter({("db", "analytics"): ["foo"]}, quoted=True)

        assert find_missing_relations(
            adapter, manifest, dict.fromkeys(manifest.nodes)
        ) == {"model.p.a"}

    def test_quoted_adapter_matches_on_exact_case(self) -> None:
        """The complement: case-sensitive must not over-reject an exact match."""
        manifest = make_manifest({"model.p.a": ("db", "analytics", "Foo")})
        adapter, _ = make_adapter({("db", "analytics"): ["Foo"]}, quoted=True)

        assert (
            find_missing_relations(adapter, manifest, dict.fromkeys(manifest.nodes))
            == set()
        )

    def test_quoted_alias_keeps_its_case(self) -> None:
        """`alias: '"MyModel"'` with quoting off: Snowflake's `_make_match_kwargs` strips
        the quotes and skips folding, so the mixed-case name is what we look for. The
        adapter owns that precedence -- we would have uppercased it.
        """
        manifest = make_manifest({"model.p.a": ("db", "analytics", '"MyModel"')})
        adapter, _ = make_adapter({("db", "analytics"): ["MyModel"]})

        def snowflake_match_kwargs(database, schema, identifier):
            if identifier.startswith('"') and identifier.endswith('"'):
                return {
                    "database": database,
                    "schema": schema,
                    "identifier": identifier.strip('"'),
                }
            return {
                "database": database,
                "schema": schema,
                "identifier": identifier.upper(),
            }

        adapter._make_match_kwargs.side_effect = snowflake_match_kwargs

        assert (
            find_missing_relations(adapter, manifest, dict.fromkeys(manifest.nodes))
            == set()
        )

    def test_space_bearing_identifiers_are_not_conflated(self) -> None:
        """Quoted names may carry leading/trailing spaces; ` x ` is not the table `x`."""
        manifest = make_manifest({"model.p.a": ("db", "analytics", " x ")})
        adapter, _ = make_adapter({("db", "analytics"): ["x"]}, quoted=True)

        assert find_missing_relations(
            adapter, manifest, dict.fromkeys(manifest.nodes)
        ) == {"model.p.a"}

    def test_differing_quote_policies_get_separate_listings(self) -> None:
        """A quoted `analytics` and an unquoted one are different schemas in the
        warehouse, and `without_identifier()` keeps their listings apart.
        """
        calls: list[tuple[str, str]] = []
        adapter = MagicMock()
        adapter.type.return_value = "postgres"
        adapter._make_match_kwargs.side_effect = lambda database, schema, identifier: {
            "database": database,
            "schema": schema,
            "identifier": identifier,
        }
        rels = {
            # identifier casing matches; only the *schema* policy differs
            "folded": FakeRelation(
                "db", "analytics", "a", policy=(False, False, False)
            ),
            "exact": FakeRelation("db", "analytics", "a", policy=(False, True, False)),
        }
        adapter.Relation.create_from.side_effect = lambda quoting, relation_config: (
            rels[relation_config["alias"]]
        )
        manifest = SimpleNamespace(
            nodes={
                k: {"alias": k, "database": "db", "schema": "analytics"} for k in rels
            }
        )

        def list_relations(database, schema):
            calls.append((database, schema))
            return [FakeRelation(database, schema, "a")]

        adapter.list_relations.side_effect = list_relations

        find_missing_relations(adapter, manifest, list(rels))

        assert len(calls) == 2, "each quote policy needs its own listing"

    def test_schema_is_listed_exactly_as_dbt_would(self) -> None:
        """`get_relation` hands the raw schema to `list_relations` and the cache matches
        case-insensitively, so we pass it through rather than folding it ourselves.
        """
        manifest = make_manifest({"model.p.a": ("db", "Analytics", "m")})
        adapter, calls = make_adapter({("db", "Analytics"): ["m"]})

        assert (
            find_missing_relations(adapter, manifest, dict.fromkeys(manifest.nodes))
            == set()
        )
        assert calls == [("db", "Analytics")]


class TestCollectReuseCandidates:
    def test_only_clean_materialisations_with_a_relation(self) -> None:
        dag = ParsedDag(
            nodes={
                "model.p.clean": _node("model.p.clean"),
                "model.p.dirty": _node("model.p.dirty", freshness=Freshness.DIRTY),
                "model.p.ephemeral": _node("model.p.ephemeral", relation_name=None),
                "source.p.s": SourceNode(),
            },
            edges=[],
        )

        assert list(collect_reuse_candidates(dag, None)) == ["model.p.clean"]

    def test_paths_to_run_narrows_the_candidates(self) -> None:
        dag = ParsedDag(
            nodes={
                "model.p.sel": _node("model.p.sel", dbt_path="models/sel.sql"),
                "model.p.exc": _node("model.p.exc", dbt_path="models/exc.sql"),
            },
            edges=[],
        )

        assert list(collect_reuse_candidates(dag, ["models/sel.sql"])) == [
            "model.p.sel"
        ]


class TestApplyRelationExistenceGate:
    @staticmethod
    def _stub_adapter(
        monkeypatch, *, adapter_type="postgres", missing=None, raises=None
    ):
        adapter = MagicMock()
        adapter.type.return_value = adapter_type
        monkeypatch.setattr(
            relation_existence,
            "_acquire_adapter",
            MagicMock(
                side_effect=raises,
                return_value=(adapter, SimpleNamespace(nodes={})),
            ),
        )
        monkeypatch.setattr(
            relation_existence,
            "find_missing_relations",
            MagicMock(return_value=missing or set()),
        )
        return adapter

    def test_no_candidates_never_touches_the_adapter(self, monkeypatch) -> None:
        acquire = MagicMock()
        monkeypatch.setattr(relation_existence, "_acquire_adapter", acquire)
        dag = ParsedDag(
            nodes={"model.p.d": _node("model.p.d", freshness=Freshness.DIRTY)}, edges=[]
        )

        apply_relation_existence_gate(dag, None)

        acquire.assert_not_called()

    @pytest.mark.parametrize(
        ("kwargs", "label"),
        [
            ({"raises": RuntimeError("no adapter")}, "adapter unavailable"),
            ({"adapter_type": "spark"}, "unsupported adapter"),
        ],
    )
    def test_safety_branches_leave_the_dag_alone(
        self, monkeypatch, kwargs, label
    ) -> None:
        self._stub_adapter(monkeypatch, **kwargs)
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        apply_relation_existence_gate(dag, None)

        node = dag.nodes["model.p.a"]
        assert isinstance(node, MaterialisationNode)
        assert node.freshness == Freshness.CLEAN, label

    def test_a_failing_check_leaves_the_dag_alone(self, monkeypatch) -> None:
        self._stub_adapter(monkeypatch)
        monkeypatch.setattr(
            relation_existence,
            "find_missing_relations",
            MagicMock(side_effect=RuntimeError("connection reset")),
        )
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        apply_relation_existence_gate(dag, None)

        node = dag.nodes["model.p.a"]
        assert isinstance(node, MaterialisationNode)
        assert node.freshness == Freshness.CLEAN

    def test_missing_relation_is_flipped_to_dirty(self, monkeypatch) -> None:
        self._stub_adapter(monkeypatch, missing={"model.p.a"})
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        apply_relation_existence_gate(dag, None)

        node = dag.nodes["model.p.a"]
        assert isinstance(node, MaterialisationNode)
        assert node.freshness == Freshness.DIRTY
        assert "deleted hence rerun" in node.reason

    def test_connection_is_always_released(self, monkeypatch) -> None:
        """The real dbt run is a subprocess started right after this."""
        adapter = self._stub_adapter(monkeypatch)

        apply_relation_existence_gate(
            ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[]), None
        )

        adapter.cleanup_connections.assert_called_once()

    def test_dropped_table_detected_via_its_alias_not_its_model_name(
        self, monkeypatch
    ) -> None:
        """A model's `alias:` config can differ from its file/model name; the check
        must key off the alias dbt actually creates in the warehouse, not the
        unique_id, or a dropped aliased table would be missed.
        """
        manifest = make_manifest(
            {"model.p.readable_name": ("db", "analytics", "legacy_alias")}
        )
        adapter, _ = make_adapter({("db", "analytics"): []})
        monkeypatch.setattr(
            relation_existence,
            "_acquire_adapter",
            MagicMock(return_value=(adapter, manifest)),
        )
        dag = ParsedDag(
            nodes={
                "model.p.readable_name": _node(
                    "model.p.readable_name",
                    relation_name="db.analytics.legacy_alias",
                )
            },
            edges=[],
        )

        apply_relation_existence_gate(dag, None)

        node = dag.nodes["model.p.readable_name"]
        assert isinstance(node, MaterialisationNode)
        assert node.freshness == Freshness.DIRTY
        assert "db.analytics.legacy_alias" in node.reason
        assert "deleted hence rerun" in node.reason

    def test_logs_how_long_the_check_took(self, monkeypatch, capsys) -> None:
        self._stub_adapter(monkeypatch, missing={"model.p.a"})
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        apply_relation_existence_gate(dag, None)

        out = capsys.readouterr().out
        assert re.search(
            r"Warehouse existence check for 1 node\(s\) took \d+\.\d\ds\.", out
        )

    @pytest.mark.parametrize(
        ("kwargs", "label"),
        [
            ({"raises": RuntimeError("no adapter")}, "adapter unavailable"),
            ({"adapter_type": "spark"}, "unsupported adapter"),
        ],
    )
    def test_timing_is_not_logged_on_safety_branches(
        self, monkeypatch, capsys, kwargs, label
    ) -> None:
        """Nothing was actually checked, so a duration would be misleading."""
        self._stub_adapter(monkeypatch, **kwargs)
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        apply_relation_existence_gate(dag, None)

        out = capsys.readouterr().out
        assert "took" not in out, label


class TestAcquireAdapter:
    @pytest.mark.parametrize(
        ("adapters", "match"),
        [
            ({}, "Expected exactly one"),
            ({"postgres": None, "snowflake": None}, "Expected exactly one"),
        ],
    )
    def test_raises_unless_exactly_one_adapter_is_registered(
        self, monkeypatch, adapters, match
    ) -> None:
        factory = pytest.importorskip("dbt.adapters.factory")
        monkeypatch.setattr(factory.FACTORY, "adapters", adapters, raising=False)

        with pytest.raises(RuntimeError, match=match):
            relation_existence._acquire_adapter()

    def test_raises_when_the_adapter_has_no_manifest(self, monkeypatch) -> None:
        factory = pytest.importorskip("dbt.adapters.factory")
        adapter = MagicMock()
        adapter.get_macro_resolver.return_value = None
        monkeypatch.setattr(
            factory.FACTORY, "adapters", {"postgres": adapter}, raising=False
        )
        monkeypatch.setattr(
            factory.FACTORY, "lookup_adapter", MagicMock(return_value=adapter)
        )

        with pytest.raises(RuntimeError, match="no manifest attached"):
            relation_existence._acquire_adapter()

    def test_returns_the_registered_adapter_and_its_manifest(self, monkeypatch) -> None:
        factory = pytest.importorskip("dbt.adapters.factory")
        manifest = SimpleNamespace(nodes={"model.p.a": {}})
        adapter = MagicMock()
        adapter.get_macro_resolver.return_value = manifest
        monkeypatch.setattr(
            factory.FACTORY, "adapters", {"postgres": adapter}, raising=False
        )
        monkeypatch.setattr(
            factory.FACTORY, "lookup_adapter", MagicMock(return_value=adapter)
        )

        assert relation_existence._acquire_adapter() == (adapter, manifest)


class TestEndToEndFromStateThroughTheGate:
    def test_aliased_model_already_in_state_is_rebuilt_when_its_relation_is_dropped(
        self, monkeypatch
    ) -> None:
        """Full chain: a real manifest with an `alias:`-configured model, a state
        file that matches its checksum (so `construct_dag` marks it CLEAN, i.e. a
        candidate for reuse), then the gate discovering the aliased table is gone.
        """
        manifest = {
            "metadata": {"project_name": "test_project"},
            "nodes": {
                "model.test_project.readable_name": {
                    "resource_type": "model",
                    "checksum": {"checksum": "abc123"},
                    "config": {},
                    "package_name": "test_project",
                    "original_file_path": "models/readable_name.sql",
                    "relation_name": "db.analytics.legacy_alias",
                    "depends_on": {"nodes": []},
                },
            },
            "child_map": {},
        }
        monkeypatch.setattr(dag_module, "load_json", lambda _: manifest)
        monkeypatch.setattr(dag_module, "calculate_checksum", lambda *a, **k: "abc123")
        monkeypatch.setattr(
            dag_module,
            "load_orchestra_dbt_settings",
            lambda: OrchestraDbtSettings(
                integration_account_id="acct", local_run=False
            ),
        )
        asset_id = "acct.db.analytics.legacy_alias"  # integration_account_id.relation_name
        state = StateApiModel(
            state={
                asset_id: StateItem(
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="abc123",
                    sources={},
                ),
            }
        )

        dag = construct_dag(SourceFreshness(sources={}), state)
        node = dag.nodes["model.test_project.readable_name"]
        assert isinstance(node, MaterialisationNode)
        assert node.freshness == Freshness.CLEAN, "already in state, so reusable"
        assert node.relation_name == "db.analytics.legacy_alias"

        manifest_for_check = make_manifest(
            {
                "model.test_project.readable_name": (
                    "db",
                    "analytics",
                    "legacy_alias",
                )
            }
        )
        adapter, _ = make_adapter({("db", "analytics"): []})
        monkeypatch.setattr(
            relation_existence,
            "_acquire_adapter",
            MagicMock(return_value=(adapter, manifest_for_check)),
        )

        apply_relation_existence_gate(dag, None)

        assert node.freshness == Freshness.DIRTY
        assert "db.analytics.legacy_alias" in node.reason
        assert "deleted hence rerun" in node.reason

    def test_stale_source_does_not_change_the_outcome_or_overwrite_the_reason(
        self, monkeypatch
    ) -> None:
        """Same scenario, plus an upstream source that has new data since last run --
        a second, independent reason to rebuild. The gate's DIRTY must survive the
        later dependency sweep (`calculate_nodes_to_run`) unchanged: `_process_node`
        only escalates CLEAN to DIRTY, it never touches a node already DIRTY.
        """
        manifest = {
            "metadata": {"project_name": "test_project"},
            "nodes": {
                "model.test_project.readable_name": {
                    "resource_type": "model",
                    "checksum": {"checksum": "abc123"},
                    "config": {},
                    "package_name": "test_project",
                    "original_file_path": "models/readable_name.sql",
                    "relation_name": "db.analytics.legacy_alias",
                    "depends_on": {"nodes": ["source.test_db.raw.events"]},
                },
            },
            "child_map": {
                "source.test_db.raw.events": ["model.test_project.readable_name"],
            },
        }
        monkeypatch.setattr(dag_module, "load_json", lambda _: manifest)
        monkeypatch.setattr(dag_module, "calculate_checksum", lambda *a, **k: "abc123")
        monkeypatch.setattr(
            dag_module,
            "load_orchestra_dbt_settings",
            lambda: OrchestraDbtSettings(
                integration_account_id="acct", local_run=False
            ),
        )
        asset_id = "acct.db.analytics.legacy_alias"
        state = StateApiModel(
            state={
                asset_id: StateItem(
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="abc123",
                    # The source had new data at 2024-01-03; state only saw 2024-01-01.
                    sources={
                        "source.test_db.raw.events": datetime(2024, 1, 1, 0, 0, 0)
                    },
                ),
            }
        )
        source_freshness = SourceFreshness(
            sources={"source.test_db.raw.events": datetime(2024, 1, 3, 0, 0, 0)}
        )

        dag = construct_dag(source_freshness, state)
        node = dag.nodes["model.test_project.readable_name"]
        assert isinstance(node, MaterialisationNode)
        assert node.freshness == Freshness.CLEAN, "checksum still matches state"

        manifest_for_check = make_manifest(
            {
                "model.test_project.readable_name": (
                    "db",
                    "analytics",
                    "legacy_alias",
                )
            }
        )
        adapter, _ = make_adapter({("db", "analytics"): []})
        monkeypatch.setattr(
            relation_existence,
            "_acquire_adapter",
            MagicMock(return_value=(adapter, manifest_for_check)),
        )

        apply_relation_existence_gate(dag, None)
        assert node.freshness == Freshness.DIRTY
        reason_after_gate = node.reason
        assert "deleted hence rerun" in reason_after_gate

        calculate_nodes_to_run(dag)

        assert node.freshness == Freshness.DIRTY
        assert node.reason == reason_after_gate, (
            "the sweep must not touch a node the gate already marked dirty"
        )
