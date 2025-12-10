from datetime import datetime, timedelta

import pytest

from src.orchestra_dbt.models import (
    Edge,
    Freshness,
    ModelNode,
    Node,
    NodeType,
    ParsedDag,
    SourceNode,
)
from src.orchestra_dbt.sao import (
    build_after_duration_minutes,
    build_dependency_graphs,
    calculate_models_to_run,
    should_mark_dirty_from_single_upstream,
)


class TestBuildDependencyGraphs:
    def test_build_dependency_graphs(self):
        assert build_dependency_graphs(
            dag=ParsedDag(
                nodes={
                    "model.a": ModelNode(
                        freshness=Freshness.CLEAN,
                        checksum="1",
                        sql_path="models/model_a.sql",
                    ),
                    "model.b": ModelNode(
                        freshness=Freshness.CLEAN,
                        checksum="2",
                        sql_path="models/model_b.sql",
                    ),
                    "model.c": ModelNode(
                        freshness=Freshness.CLEAN,
                        checksum="3",
                        sql_path="models/model_c.sql",
                    ),
                    "model.d": ModelNode(
                        freshness=Freshness.CLEAN,
                        checksum="4",
                        sql_path="models/model_d.sql",
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


class TestBuildAfterDurationMinutes:
    @pytest.mark.parametrize(
        "period, count, expected",
        [("minute", 30, 30), ("hour", 2, 120), ("day", 1, 1440), ("minute", 0, 0)],
    )
    def test_build_after_duration_minutes(self, period: str, count: int, expected: int):
        assert (
            build_after_duration_minutes({"period": period, "count": count}) == expected
        )

    def test_build_after_duration_minutes_invalid_period(self):
        with pytest.raises(ValueError, match="Invalid period"):
            build_after_duration_minutes({"period": "invalid", "count": 1})

    def test_build_after_duration_minutes_invalid_count(self):
        with pytest.raises(ValueError, match="Invalid count"):
            build_after_duration_minutes({"period": "minute", "count": "invalid"})


class TestShouldMarkDirtyFromSingleUpstream:
    @pytest.mark.parametrize(
        "upstream_id, upstream_node, current_node, expected",
        [
            # Current source has no last updated at.
            (
                "source.test",
                SourceNode(
                    type=NodeType.SOURCE,
                ),
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    type=NodeType.MODEL,
                    sources={
                        "source.test": datetime.now(),
                    },
                ),
                True,
            ),
            # Clean Source -> Model no config
            (
                "source.test",
                SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=datetime.now() - timedelta(minutes=10),
                ),
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=10),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                ),
                False,
            ),
            # Dirty Source -> Model no config
            (
                "source.test",
                SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=datetime.now(),
                ),
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=10),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                ),
                True,
            ),
            # Clean Source -> Model with invalid config
            (
                "source.test",
                SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=datetime.now() - timedelta(minutes=10),
                ),
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=10),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                    freshness_config={
                        "erm": {
                            "foo": "bar",
                        }
                    },
                ),
                False,
            ),
            # Dirty Source -> Model should not be built yet
            (
                "source.test",
                SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=datetime.now(),
                ),
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=10),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                    freshness_config={
                        "build_after": {
                            "count": 15,
                            "period": "minute",
                        }
                    },
                ),
                False,
            ),
            # Dirty Source -> Model should be built again
            (
                "source.test",
                SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=datetime.now(),
                ),
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=20),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=20),
                    },
                    freshness_config={
                        "build_after": {
                            "count": 15,
                            "period": "minute",
                        }
                    },
                ),
                True,
            ),
            # Clean parent Model -> parent updated a while ago
            (
                "model.a",
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=60),
                ),
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_b.sql",
                    last_updated=datetime.now() - timedelta(minutes=20),
                    sources={},
                    freshness_config={
                        "build_after": {
                            "count": 15,
                            "period": "minute",
                        }
                    },
                ),
                False,
            ),
            # Clean parent Model -> parent updated recently
            (
                "model.a",
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=12),
                ),
                ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_b.sql",
                    last_updated=datetime.now() - timedelta(minutes=20),
                    sources={},
                    freshness_config={
                        "build_after": {
                            "count": 15,
                            "period": "minute",
                        }
                    },
                ),
                True,
            ),
        ],
    )
    def test_should_mark_dirty_from_single_upstream(
        self,
        upstream_id: str,
        upstream_node: Node,
        current_node: ModelNode,
        expected: bool,
    ):
        assert (
            should_mark_dirty_from_single_upstream(
                upstream_id=upstream_id,
                upstream_node=upstream_node,
                current_node=current_node,
            )
            is expected
        )


class TestCalculateModelsToRun:
    def test_calculate_models_to_run_propagates_dirty(self):
        # Create a simple DAG: source -> model_a -> model_b
        dag = ParsedDag(
            nodes={
                "source.test": SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=datetime.now(),
                ),
                "model.a": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                ),
                "model.b": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    sql_path="models/model_b.sql",
                ),
            },
            edges=[
                Edge(from_="source.test", to_="model.a"),
                Edge(from_="model.a", to_="model.b"),
            ],
        )
        calculate_models_to_run(dag=dag)

        # Both models should be dirty now
        assert (
            isinstance(dag.nodes["model.a"], ModelNode)
            and dag.nodes["model.a"].freshness == Freshness.DIRTY
        )
        assert (
            isinstance(dag.nodes["model.b"], ModelNode)
            and dag.nodes["model.b"].freshness == Freshness.DIRTY
        )

    def test_calculate_models_to_run_preserves_clean(self):
        dag = ParsedDag(
            nodes={
                "source.test": SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=datetime.now() - timedelta(minutes=10),
                ),
                "model.a": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    last_updated=datetime.now() - timedelta(minutes=5),
                    sources={
                        "source.test": datetime.now() - timedelta(minutes=10),
                    },
                ),
            },
            edges=[Edge(from_="source.test", to_="model.a")],
        )
        calculate_models_to_run(dag=dag)

        # Model should remain clean
        assert (
            isinstance(dag.nodes["model.a"], ModelNode)
            and dag.nodes["model.a"].freshness == Freshness.CLEAN
        )

    def test_calculate_models_to_run_respects_build_after(self):
        dag = ParsedDag(
            nodes={
                "source.test": SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=datetime.now(),
                ),
                "model.a": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    freshness_config={"build_after": {"count": 1, "period": "hour"}},
                    last_updated=datetime.now() - timedelta(minutes=20),
                ),
            },
            edges=[Edge(from_="source.test", to_="model.a")],
        )
        calculate_models_to_run(dag=dag)

        # Model should remain clean due to build_after config
        assert (
            isinstance(dag.nodes["model.a"], ModelNode)
            and dag.nodes["model.a"].freshness == Freshness.CLEAN
        )

    @pytest.mark.parametrize(
        "updates_on, result_freshness",
        [("any", Freshness.DIRTY), ("all", Freshness.CLEAN)],
    )
    def test_calculate_models_to_run_with_updates_on_any_or_all(
        self, updates_on: str, result_freshness: Freshness
    ):
        now = datetime.now()

        dag = ParsedDag(
            nodes={
                "model.a": ModelNode(
                    freshness=Freshness.DIRTY,
                    checksum="1",
                    sql_path="models/model_a.sql",
                    last_updated=now,
                ),
                "model.b": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    sql_path="models/model_b.sql",
                    last_updated=now - timedelta(hours=2),  # Old, shouldn't trigger
                ),
                "model.c": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    sql_path="models/model_c.sql",
                    freshness_config={
                        "build_after": {
                            "count": 30,
                            "period": "minute",
                            "updates_on": updates_on,
                        }
                    },
                    last_updated=now - timedelta(minutes=45),
                ),
            },
            edges=[
                Edge(from_="model.a", to_="model.c"),
                Edge(from_="model.b", to_="model.c"),
            ],
        )

        calculate_models_to_run(dag=dag)
        assert (
            isinstance(dag.nodes["model.c"], ModelNode)
            and dag.nodes["model.c"].freshness == result_freshness
        )

    def test_calculate_models_to_run_sample_1(self):
        now = datetime.now()

        dag = ParsedDag(
            nodes={
                "source.src_orders": SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=now - timedelta(minutes=10),
                ),
                "source.src_customers": SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=now,
                ),
                "model.stg_orders": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_stg_orders.sql",
                    last_updated=now - timedelta(minutes=9),
                    sources={
                        "source.src_orders": now - timedelta(minutes=10),
                    },
                ),
                "model.stg_customers": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    sql_path="models/model_stg_customers.sql",
                    last_updated=now - timedelta(minutes=9),
                    sources={
                        "source.src_customers": now - timedelta(minutes=10),
                    },
                ),
                "model.int_orders": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    sql_path="models/model_int_orders.sql",
                    last_updated=now - timedelta(minutes=8),
                ),
                "model.dim_customers": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="4",
                    sql_path="models/model_dim_customers.sql",
                    last_updated=now - timedelta(minutes=8),
                ),
                "model.cust_orders": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="5",
                    sql_path="models/model_cust_orders.sql",
                    last_updated=now - timedelta(minutes=7),
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

        calculate_models_to_run(dag=dag)

        # Reused
        assert (
            isinstance(dag.nodes["model.stg_orders"], ModelNode)
            and dag.nodes["model.stg_orders"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.int_orders"], ModelNode)
            and dag.nodes["model.int_orders"].freshness == Freshness.CLEAN
        )

        # Rebuilt
        assert (
            isinstance(dag.nodes["model.stg_customers"], ModelNode)
            and dag.nodes["model.stg_customers"].freshness == Freshness.DIRTY
        )
        assert (
            isinstance(dag.nodes["model.cust_orders"], ModelNode)
            and dag.nodes["model.cust_orders"].freshness == Freshness.DIRTY
        )
        assert (
            isinstance(dag.nodes["model.dim_customers"], ModelNode)
            and dag.nodes["model.dim_customers"].freshness == Freshness.DIRTY
        )

    def test_calculate_models_to_run_sample_2(self):
        now = datetime.now()

        dag = ParsedDag(
            nodes={
                "source.src_orders": SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=now - timedelta(minutes=10),
                ),
                "source.src_customers": SourceNode(
                    type=NodeType.SOURCE,
                    last_updated=now - timedelta(minutes=5),
                ),
                "model.stg_orders": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    sql_path="models/model_stg_orders.sql",
                    last_updated=now - timedelta(minutes=4),
                    sources={
                        "source.src_orders": now - timedelta(minutes=10),
                    },
                ),
                "model.stg_customers": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    sql_path="models/model_stg_customers.sql",
                    last_updated=now - timedelta(minutes=4),
                    freshness_config={
                        "build_after": {
                            "count": 7,
                            "period": "minute",
                        }
                    },
                    sources={
                        "source.src_customers": now - timedelta(minutes=6),
                    },
                ),
                "model.int_orders": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    sql_path="models/model_int_orders.sql",
                    last_updated=now - timedelta(minutes=3),
                ),
                "model.dim_customers": ModelNode(
                    freshness=Freshness.CLEAN,
                    checksum="4",
                    sql_path="models/model_dim_customers.sql",
                    last_updated=now - timedelta(minutes=3),
                ),
                "model.cust_orders": ModelNode(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    sql_path="models/model_cust_orders.sql",
                    checksum="5",
                    last_updated=now - timedelta(minutes=2),
                    freshness_config={
                        "build_after": {
                            "count": 7,
                            "period": "minute",
                            "updates_on": "all",
                        }
                    },
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

        calculate_models_to_run(dag=dag)

        # All reused
        assert (
            isinstance(dag.nodes["model.stg_orders"], ModelNode)
            and dag.nodes["model.stg_orders"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.int_orders"], ModelNode)
            and dag.nodes["model.int_orders"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.stg_customers"], ModelNode)
            and dag.nodes["model.stg_customers"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.cust_orders"], ModelNode)
            and dag.nodes["model.cust_orders"].freshness == Freshness.CLEAN
        )
        assert (
            isinstance(dag.nodes["model.dim_customers"], ModelNode)
            and dag.nodes["model.dim_customers"].freshness == Freshness.CLEAN
        )
