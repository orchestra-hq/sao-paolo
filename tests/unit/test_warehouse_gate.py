from datetime import datetime, timezone

from src.orchestra_dbt.models import (
    Edge,
    Freshness,
    FreshnessConfig,
    MaterialisationNode,
    ParsedDag,
)
from src.orchestra_dbt.sao import calculate_nodes_to_run
from src.orchestra_dbt.warehouse.existence import RelationExistence
from src.orchestra_dbt.warehouse.gate import mark_missing_relations_dirty


def _materialisation(dag: ParsedDag, node_id: str) -> MaterialisationNode:
    """Narrow ParsedDag's `Node` values to the subclass the assertions need."""
    node = dag.nodes[node_id]
    assert isinstance(node, MaterialisationNode)
    return node


def _node(
    node_id: str,
    freshness: Freshness = Freshness.CLEAN,
    relation_name: str | None = None,
    last_updated: datetime | None = None,
) -> MaterialisationNode:
    return MaterialisationNode(
        asset_external_id=node_id,
        checksum="abc",
        dbt_path=f"models/{node_id}.sql",
        file_path=f"models/{node_id}.sql",
        freshness_config=FreshnessConfig(),
        freshness=freshness,
        reason="Model in same state as last run.",
        sources={},
        last_updated=last_updated or datetime(2024, 1, 1, tzinfo=timezone.utc),
        relation_name=relation_name or f'"db"."schema"."{node_id}"',
    )


class TestMarkMissingRelationsDirty:
    def test_missing_clean_node_is_flipped_with_a_reason(self) -> None:
        dag = ParsedDag(nodes={"model.p.a": _node("model.p.a")}, edges=[])

        flipped = mark_missing_relations_dirty(
            dag, {"model.p.a": RelationExistence.MISSING}
        )

        assert flipped == 1
        node = _materialisation(dag, "model.p.a")
        assert node.freshness == Freshness.DIRTY
        assert '"db"."schema"."model.p.a"' in node.reason
        assert "deleted hence rerun" in node.reason

    def test_falls_back_to_node_id_when_relation_name_is_unset(self) -> None:
        node = _node("model.p.a")
        node.relation_name = None
        dag = ParsedDag(nodes={"model.p.a": node}, edges=[])

        mark_missing_relations_dirty(dag, {"model.p.a": RelationExistence.MISSING})

        assert "model.p.a" in _materialisation(dag, "model.p.a").reason

    def test_exists_and_unknown_leave_the_node_alone(self) -> None:
        dag = ParsedDag(
            nodes={"model.p.a": _node("model.p.a"), "model.p.b": _node("model.p.b")},
            edges=[],
        )
        original_reason = _materialisation(dag, "model.p.a").reason

        flipped = mark_missing_relations_dirty(
            dag,
            {
                "model.p.a": RelationExistence.EXISTS,
                "model.p.b": RelationExistence.UNKNOWN,
            },
        )

        assert flipped == 0
        assert _materialisation(dag, "model.p.a").freshness == Freshness.CLEAN
        assert _materialisation(dag, "model.p.a").reason == original_reason
        assert _materialisation(dag, "model.p.b").freshness == Freshness.CLEAN

    def test_already_dirty_node_is_not_recounted(self) -> None:
        dag = ParsedDag(
            nodes={"model.p.a": _node("model.p.a", freshness=Freshness.DIRTY)}, edges=[]
        )

        assert (
            mark_missing_relations_dirty(dag, {"model.p.a": RelationExistence.MISSING})
            == 0
        )

    def test_unknown_node_ids_are_ignored(self) -> None:
        dag = ParsedDag(nodes={}, edges=[])

        assert (
            mark_missing_relations_dirty(
                dag, {"model.p.gone": RelationExistence.MISSING}
            )
            == 0
        )


class TestPropagationThroughSao:
    def test_forced_dirty_node_dirties_its_children(self) -> None:
        """The gate runs before calculate_nodes_to_run so downstream rebuild is automatic."""
        parent = _node("model.p.parent")
        child = _node("model.p.child")
        grandchild = _node("model.p.grandchild")
        dag = ParsedDag(
            nodes={
                "model.p.parent": parent,
                "model.p.child": child,
                "model.p.grandchild": grandchild,
            },
            edges=[
                Edge(from_="model.p.parent", to_="model.p.child"),
                Edge(from_="model.p.child", to_="model.p.grandchild"),
            ],
        )

        mark_missing_relations_dirty(dag, {"model.p.parent": RelationExistence.MISSING})
        calculate_nodes_to_run(dag)

        assert _materialisation(dag, "model.p.parent").freshness == Freshness.DIRTY
        assert _materialisation(dag, "model.p.child").freshness == Freshness.DIRTY
        assert _materialisation(dag, "model.p.grandchild").freshness == Freshness.DIRTY

    def test_unrelated_clean_nodes_are_untouched(self) -> None:
        dag = ParsedDag(
            nodes={
                "model.p.missing": _node("model.p.missing"),
                "model.p.other": _node("model.p.other"),
            },
            edges=[],
        )

        mark_missing_relations_dirty(
            dag, {"model.p.missing": RelationExistence.MISSING}
        )
        calculate_nodes_to_run(dag)

        assert _materialisation(dag, "model.p.missing").freshness == Freshness.DIRTY
        assert _materialisation(dag, "model.p.other").freshness == Freshness.CLEAN
