from datetime import datetime, timedelta
from typing import Literal

import pytest

from src.orchestra_dbt.models import (
    Edge,
    Freshness,
    FreshnessConfig,
    MaterialisationNode,
    Node,
    ParsedDag,
    SourceNode,
)
from src.orchestra_dbt.sao import (
    build_dependency_graphs,
    calculate_nodes_to_run,
    should_mark_dirty_from_single_upstream,
)


class TestBuildDependencyGraphs:
    def test_build_dependency_graphs(self):
        assert build_dependency_graphs(
            dag=ParsedDag(
                nodes={
                    "model.a": MaterialisationNode(
                        freshness=Freshness.CLEAN,
                        checksum="1",
                        node_path="models/model_a.sql",
                        sql_path="models/model_a.sql",
                        reason="Node not seen before",
                        sources={},
                        freshness_config=FreshnessConfig(),
                    ),
                    "model.b": MaterialisationNode(
                        freshness=Freshness.CLEAN,
                        checksum="2",
                        node_path="models/model_b.sql",
                        sql_path="models/model_b.sql",
                        reason="Node not seen before",
                        sources={},
                        freshness_config=FreshnessConfig(),
                    ),
                    "model.c": MaterialisationNode(
                        freshness=Freshness.CLEAN,
                        checksum="3",
                        node_path="models/model_c.sql",
                        sql_path="models/model_c.sql",
                        reason="Node not seen before",
                        sources={},
                        freshness_config=FreshnessConfig(),
                    ),
                    "model.d": MaterialisationNode(
                        freshness=Freshness.CLEAN,
                        checksum="4",
                        node_path="models/model_d.sql",
                        sql_path="models/model_d.sql",
                        reason="Node not seen before",
                        sources={},
                        freshness_config=FreshnessConfig(),
                    ),
                },
                edges=[
                    Edge(from_="model.a", to_="model.b"),
                    Edge(from_="model.a", to_="model.c"),
                    Edge(from_="model.b", to_="model.c"),
                    Edge(from_="model.b", to_="model.d"),
                ],
            )
        ) == (
            {
                "model.a": ["model.b", "model.c"],
                "model.b": ["model.c", "model.d"],
            },
            {
                "model.b": ["model.a"],
                "model.c": ["model.a", "model.b"],
                "model.d": ["model.b"],
            },
            {
                "model.a": 0,
                "model.b": 1,
                "model.c": 2,
                "model.d": 1,
            },
        )


class TestShouldMarkDirtyFromSingleUpstream:
    @pytest.mark.parametrize(
        "upstream_id, upstream_node, current_node, expected",
        [
            # Current source has no last updated at.
            (
                "source.test",
                SourceNode(),
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    sources={
                        "source.test": datetime.now(),
                    },
                    reason="Node not seen before",
                    freshness_config=FreshnessConfig(),
                ),
                (True, None),
            ),
            # Clean Source -> Model no config
            (
                "source.test",
                SourceNode(
                    last_updated=datetime.now() - timedelta(minutes=10),
                ),
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=10),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                    reason="Node not seen before",
                    freshness_config=FreshnessConfig(),
                ),
                (False, "Source source.test has not been updated since last run."),
            ),
            # Dirty Source -> Model no config
            (
                "source.test",
                SourceNode(last_updated=datetime.now()),
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=10),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                    reason="Node not seen before",
                    freshness_config=FreshnessConfig(),
                ),
                (True, None),
            ),
            # Clean Source -> Model with invalid config
            (
                "source.test",
                SourceNode(last_updated=datetime.now() - timedelta(minutes=10)),
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=10),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                    freshness_config=FreshnessConfig(),
                    reason="Node not seen before",
                ),
                (False, "Source source.test has not been updated since last run."),
            ),
            # Dirty Source -> Model should not be built yet
            (
                "source.test",
                SourceNode(last_updated=datetime.now()),
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=10),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                    freshness_config=FreshnessConfig(
                        minutes_sla=15, inherited_from="model.c"
                    ),
                    reason="Node not seen before",
                ),
                (
                    False,
                    "Model still within build_after config of 15 minutes. Inherited from model.c.",
                ),
            ),
            # Dirty Source -> Model should be built again
            (
                "source.test",
                SourceNode(last_updated=datetime.now()),
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=20),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=20),
                    },
                    freshness_config=FreshnessConfig(minutes_sla=15),
                    reason="Node not seen before",
                ),
                (True, None),
            ),
            # Clean parent Model -> parent updated a while ago
            (
                "model.a",
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=60),
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_b.sql",
                    sql_path="models/model_b.sql",
                    last_updated=datetime.now() - timedelta(minutes=20),
                    sources={},
                    freshness_config=FreshnessConfig(minutes_sla=15),
                    reason="Node not seen before",
                ),
                (False, "Upstream node(s) being reused."),
            ),
            # Clean parent Model -> clean child
            (
                "model.a",
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=20),
                    reason="Same state as before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_b.sql",
                    sql_path="models/model_b.sql",
                    last_updated=datetime.now() - timedelta(minutes=20),
                    reason="Same state as before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                (False, "Upstream node(s) being reused."),
            ),
            # Clean parent Model -> parent updated more recently than the current node
            (
                "model.a",
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=12),
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_b.sql",
                    sql_path="models/model_b.sql",
                    last_updated=datetime.now() - timedelta(minutes=20),
                    sources={},
                    freshness_config=FreshnessConfig(minutes_sla=15),
                    reason="Node not seen before",
                ),
                (True, None),
            ),
        ],
    )
    def test_should_mark_dirty_from_single_upstream(
        self,
        upstream_id: str,
        upstream_node: Node,
        current_node: MaterialisationNode,
        expected: tuple[bool, str | None],
    ):
        assert (
            should_mark_dirty_from_single_upstream(
                upstream_id=upstream_id,
                upstream_node=upstream_node,
                current_node=current_node,
            )
            == expected
        )


class TestCalculateModelsToRun:
    def test_calculate_nodes_to_run_propagates_dirty(self):
        # Create a simple DAG: source -> model_a -> model_b
        dag = ParsedDag(
            nodes={
                "source.test": SourceNode(
                    last_updated=datetime.now(),
                ),
                "model.a": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                "model.b": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/model_b.sql",
                    sql_path="models/model_b.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
            },
            edges=[
                Edge(from_="source.test", to_="model.a"),
                Edge(from_="model.a", to_="model.b"),
            ],
        )
        calculate_nodes_to_run(dag=dag)

        # Both models should be dirty now
        assert (
            isinstance(dag.nodes["model.a"], MaterialisationNode)
            and dag.nodes["model.a"].freshness == Freshness.DIRTY
        )
        assert (
            isinstance(dag.nodes["model.b"], MaterialisationNode)
            and dag.nodes["model.b"].freshness == Freshness.DIRTY
        )

    def test_calculate_nodes_to_run_preserves_clean(self):
        dag = ParsedDag(
            nodes={
                "source.test": SourceNode(
                    last_updated=datetime.now() - timedelta(minutes=10),
                ),
                "model.a": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=5),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                    reason="Node not seen before",
                    freshness_config=FreshnessConfig(),
                ),
            },
            edges=[Edge(from_="source.test", to_="model.a")],
        )
        calculate_nodes_to_run(dag=dag)

        # Model should remain clean
        assert (
            isinstance(dag.nodes["model.a"], MaterialisationNode)
            and dag.nodes["model.a"].freshness == Freshness.CLEAN
        )

    def test_calculate_nodes_to_run_respects_build_after(self):
        dag = ParsedDag(
            nodes={
                "source.test": SourceNode(
                    last_updated=datetime.now(),
                ),
                "model.a": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    freshness_config=FreshnessConfig(minutes_sla=60),
                    last_updated=datetime.now() - timedelta(minutes=20),
                    reason="Node not seen before",
                    sources={},
                ),
            },
            edges=[Edge(from_="source.test", to_="model.a")],
        )
        calculate_nodes_to_run(dag=dag)

        # Model should remain clean due to build_after config
        assert (
            isinstance(dag.nodes["model.a"], MaterialisationNode)
            and dag.nodes["model.a"].freshness == Freshness.CLEAN
        )

    @pytest.mark.parametrize(
        "updates_on, result_freshness",
        [("any", Freshness.DIRTY), ("all", Freshness.CLEAN)],
    )
    def test_calculate_nodes_to_run_with_updates_on_any_or_all(
        self, updates_on: Literal["any", "all"], result_freshness: Freshness
    ):
        now = datetime.now()

        dag = ParsedDag(
            nodes={
                "model.a": MaterialisationNode(
                    freshness=Freshness.DIRTY,
                    checksum="1",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    last_updated=now,
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                "model.b": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/model_b.sql",
                    sql_path="models/model_b.sql",
                    last_updated=now - timedelta(hours=2),  # Old, shouldn't trigger
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                "model.c": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    node_path="models/model_c.sql",
                    sql_path="models/model_c.sql",
                    freshness_config=FreshnessConfig(
                        minutes_sla=30, updates_on=updates_on
                    ),
                    last_updated=now - timedelta(minutes=45),
                    reason="Node not seen before",
                    sources={},
                ),
            },
            edges=[
                Edge(from_="model.a", to_="model.c"),
                Edge(from_="model.b", to_="model.c"),
            ],
        )

        calculate_nodes_to_run(dag=dag)
        assert (
            isinstance(dag.nodes["model.c"], MaterialisationNode)
            and dag.nodes["model.c"].freshness == result_freshness
        )

    def test_calculate_nodes_to_run_sample_1(self):
        now = datetime.now()

        dag = ParsedDag(
            nodes={
                "source.src_orders": SourceNode(
                    last_updated=now - timedelta(minutes=10),
                ),
                "source.src_customers": SourceNode(
                    last_updated=now,
                ),
                "model.stg_orders": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_stg_orders.sql",
                    sql_path="models/model_stg_orders.sql",
                    last_updated=now - timedelta(minutes=9),
                    sources={
                        "source.src_orders": now - timedelta(minutes=10),
                    },
                    reason="Node not seen before",
                    freshness_config=FreshnessConfig(),
                ),
                "model.stg_customers": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/model_stg_customers.sql",
                    sql_path="models/model_stg_customers.sql",
                    last_updated=now - timedelta(minutes=9),
                    sources={
                        "source.src_customers": now - timedelta(minutes=10),
                    },
                    reason="Node not seen before",
                    freshness_config=FreshnessConfig(),
                ),
                "model.int_orders": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    node_path="models/model_int_orders.sql",
                    sql_path="models/model_int_orders.sql",
                    last_updated=now - timedelta(minutes=8),
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                "model.dim_customers": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="4",
                    node_path="models/model_dim_customers.sql",
                    sql_path="models/model_dim_customers.sql",
                    last_updated=now - timedelta(minutes=8),
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                "model.cust_orders": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="5",
                    node_path="models/model_cust_orders.sql",
                    sql_path="models/model_cust_orders.sql",
                    last_updated=now - timedelta(minutes=7),
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
            },
            edges=[
                Edge(from_="source.src_orders", to_="model.stg_orders"),
                Edge(from_="source.src_customers", to_="model.stg_customers"),
                Edge(from_="model.stg_orders", to_="model.int_orders"),
                Edge(from_="model.stg_customers", to_="model.dim_customers"),
                Edge(from_="model.int_orders", to_="model.cust_orders"),
                Edge(from_="model.dim_customers", to_="model.cust_orders"),
            ],
        )

        calculate_nodes_to_run(dag=dag)

        # Reused
        assert (
            isinstance(dag.nodes["model.stg_orders"], MaterialisationNode)
            and dag.nodes["model.stg_orders"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.int_orders"], MaterialisationNode)
            and dag.nodes["model.int_orders"].freshness == Freshness.CLEAN
        )

        # Rebuilt
        assert (
            isinstance(dag.nodes["model.stg_customers"], MaterialisationNode)
            and dag.nodes["model.stg_customers"].freshness == Freshness.DIRTY
        )
        assert (
            isinstance(dag.nodes["model.cust_orders"], MaterialisationNode)
            and dag.nodes["model.cust_orders"].freshness == Freshness.DIRTY
        )
        assert (
            isinstance(dag.nodes["model.dim_customers"], MaterialisationNode)
            and dag.nodes["model.dim_customers"].freshness == Freshness.DIRTY
        )

    def test_calculate_nodes_to_run_sample_2(self):
        now = datetime.now()

        dag = ParsedDag(
            nodes={
                "source.src_orders": SourceNode(
                    last_updated=now - timedelta(minutes=10),
                ),
                "source.src_customers": SourceNode(
                    last_updated=now - timedelta(minutes=5),
                ),
                "model.stg_orders": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/model_stg_orders.sql",
                    sql_path="models/model_stg_orders.sql",
                    last_updated=now - timedelta(minutes=4),
                    sources={
                        "source.src_orders": now - timedelta(minutes=10),
                    },
                    reason="Node not seen before",
                    freshness_config=FreshnessConfig(),
                ),
                "model.stg_customers": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/model_stg_customers.sql",
                    sql_path="models/model_stg_customers.sql",
                    last_updated=now - timedelta(minutes=4),
                    freshness_config=FreshnessConfig(minutes_sla=7),
                    sources={
                        "source.src_customers": now - timedelta(minutes=6),
                    },
                    reason="Node not seen before",
                ),
                "model.int_orders": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    node_path="models/model_int_orders.sql",
                    sql_path="models/model_int_orders.sql",
                    last_updated=now - timedelta(minutes=3),
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                "model.dim_customers": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="4",
                    node_path="models/model_dim_customers.sql",
                    sql_path="models/model_dim_customers.sql",
                    last_updated=now - timedelta(minutes=3),
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                "model.cust_orders": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    node_path="models/model_cust_orders.sql",
                    sql_path="models/model_cust_orders.sql",
                    checksum="5",
                    last_updated=now - timedelta(minutes=2),
                    freshness_config=FreshnessConfig(minutes_sla=7, updates_on="all"),
                    reason="Node not seen before",
                    sources={},
                ),
            },
            edges=[
                Edge(from_="source.src_orders", to_="model.stg_orders"),
                Edge(from_="source.src_customers", to_="model.stg_customers"),
                Edge(from_="model.stg_orders", to_="model.int_orders"),
                Edge(from_="model.stg_customers", to_="model.dim_customers"),
                Edge(from_="model.int_orders", to_="model.cust_orders"),
                Edge(from_="model.dim_customers", to_="model.cust_orders"),
            ],
        )

        calculate_nodes_to_run(dag=dag)

        # All reused
        assert (
            isinstance(dag.nodes["model.stg_orders"], MaterialisationNode)
            and dag.nodes["model.stg_orders"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.int_orders"], MaterialisationNode)
            and dag.nodes["model.int_orders"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.stg_customers"], MaterialisationNode)
            and dag.nodes["model.stg_customers"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.cust_orders"], MaterialisationNode)
            and dag.nodes["model.cust_orders"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.dim_customers"], MaterialisationNode)
            and dag.nodes["model.dim_customers"].freshness == Freshness.CLEAN
        )
