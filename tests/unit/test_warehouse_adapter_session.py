from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import src.orchestra_dbt.warehouse as warehouse
from src.orchestra_dbt.models import (
    Freshness,
    FreshnessConfig,
    MaterialisationNode,
    ParsedDag,
    SourceNode,
)
from src.orchestra_dbt.warehouse.adapter_session import (
    AdapterUnavailable,
    acquire_in_process_adapter,
    adapter_connection,
)


def _materialisation(dag: ParsedDag, node_id: str) -> MaterialisationNode:
    """Narrow ParsedDag's `Node` values to the subclass the assertions need."""
    node = dag.nodes[node_id]
    assert isinstance(node, MaterialisationNode)
    return node


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


class TestAcquireInProcessAdapter:
    def test_raises_when_no_adapter_is_registered(self, monkeypatch) -> None:
        factory = pytest.importorskip("dbt.adapters.factory")
        monkeypatch.setattr(factory.FACTORY, "adapters", {}, raising=False)

        with pytest.raises(AdapterUnavailable, match="Expected exactly one"):
            acquire_in_process_adapter()

    def test_raises_when_several_adapters_are_registered(self, monkeypatch) -> None:
        factory = pytest.importorskip("dbt.adapters.factory")
        monkeypatch.setattr(
            factory.FACTORY,
            "adapters",
            {"postgres": MagicMock(), "snowflake": MagicMock()},
            raising=False,
        )

        with pytest.raises(AdapterUnavailable, match="Expected exactly one"):
            acquire_in_process_adapter()

    def test_raises_when_lookup_fails(self, monkeypatch) -> None:
        factory = pytest.importorskip("dbt.adapters.factory")
        monkeypatch.setattr(
            factory.FACTORY, "adapters", {"postgres": MagicMock()}, raising=False
        )
        monkeypatch.setattr(
            factory.FACTORY,
            "lookup_adapter",
            MagicMock(side_effect=KeyError("postgres")),
        )

        with pytest.raises(AdapterUnavailable, match="No dbt adapter registered"):
            acquire_in_process_adapter()

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

        with pytest.raises(AdapterUnavailable, match="no manifest attached"):
            acquire_in_process_adapter()

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

        assert acquire_in_process_adapter() == (adapter, manifest)


class TestAdapterConnection:
    def test_connections_are_always_released(self) -> None:
        adapter = MagicMock()

        with adapter_connection(adapter):
            pass

        adapter.connection_named.assert_called_once_with("orchestra_relation_existence")
        adapter.clear_transaction.assert_called_once()
        adapter.cleanup_connections.assert_called_once()

    def test_connections_are_released_after_a_failure(self) -> None:
        adapter = MagicMock()

        with pytest.raises(ValueError):
            with adapter_connection(adapter):
                raise ValueError("boom")

        adapter.cleanup_connections.assert_called_once()


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

        assert warehouse.collect_reuse_candidates(dag, None) == ["model.p.clean"]

    def test_paths_to_run_narrows_the_candidates(self) -> None:
        dag = ParsedDag(
            nodes={
                "model.p.selected": _node(
                    "model.p.selected", dbt_path="models/selected.sql"
                ),
                "model.p.excluded": _node(
                    "model.p.excluded", dbt_path="models/excluded.sql"
                ),
            },
            edges=[],
        )

        assert warehouse.collect_reuse_candidates(dag, ["models/selected.sql"]) == [
            "model.p.selected"
        ]


class TestApplyRelationExistenceGate:
    def test_no_candidates_never_touches_the_adapter(self, monkeypatch) -> None:
        acquire = MagicMock()
        monkeypatch.setattr(warehouse, "acquire_in_process_adapter", acquire)
        dag = ParsedDag(
            nodes={"model.p.dirty": _node("model.p.dirty", freshness=Freshness.DIRTY)},
            edges=[],
        )

        warehouse.apply_relation_existence_gate(dag, None)

        acquire.assert_not_called()

    def test_unavailable_adapter_leaves_the_dag_alone(self, monkeypatch) -> None:
        monkeypatch.setattr(
            warehouse,
            "acquire_in_process_adapter",
            MagicMock(side_effect=AdapterUnavailable("no adapter")),
        )
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        warehouse.apply_relation_existence_gate(dag, None)

        assert _materialisation(dag, "model.p.a").freshness == Freshness.CLEAN

    def test_unsupported_adapter_is_skipped_before_connecting(
        self, monkeypatch
    ) -> None:
        adapter = MagicMock()
        adapter.type.return_value = "spark"
        monkeypatch.setattr(
            warehouse,
            "acquire_in_process_adapter",
            MagicMock(return_value=(adapter, SimpleNamespace(nodes={}))),
        )
        check = MagicMock()
        monkeypatch.setattr(warehouse, "check_relations_exist", check)
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        warehouse.apply_relation_existence_gate(dag, None)

        check.assert_not_called()
        assert _materialisation(dag, "model.p.a").freshness == Freshness.CLEAN

    def test_a_failing_check_leaves_the_dag_alone(self, monkeypatch) -> None:
        adapter = MagicMock()
        adapter.type.return_value = "postgres"
        monkeypatch.setattr(
            warehouse,
            "acquire_in_process_adapter",
            MagicMock(return_value=(adapter, SimpleNamespace(nodes={}))),
        )
        monkeypatch.setattr(
            warehouse,
            "check_relations_exist",
            MagicMock(side_effect=RuntimeError("connection reset")),
        )
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        warehouse.apply_relation_existence_gate(dag, None)

        assert _materialisation(dag, "model.p.a").freshness == Freshness.CLEAN

    def test_missing_relation_is_flipped_to_dirty(self, monkeypatch) -> None:
        adapter = MagicMock()
        adapter.type.return_value = "postgres"
        monkeypatch.setattr(
            warehouse,
            "acquire_in_process_adapter",
            MagicMock(return_value=(adapter, SimpleNamespace(nodes={}))),
        )
        monkeypatch.setattr(
            warehouse,
            "check_relations_exist",
            MagicMock(return_value={"model.p.a": warehouse.RelationExistence.MISSING}),
        )
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        warehouse.apply_relation_existence_gate(dag, None)

        assert _materialisation(dag, "model.p.a").freshness == Freshness.DIRTY
        assert "deleted hence rerun" in _materialisation(dag, "model.p.a").reason
