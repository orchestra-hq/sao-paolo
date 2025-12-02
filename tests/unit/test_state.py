from datetime import datetime, timedelta

import pytest

from src.orchestra_dbt.models import (
    Edge,
    Freshness,
    Node,
    NodeType,
    ParsedDag,
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
                    "model.a": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                    ),
                    "model.b": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                    ),
                    "model.c": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                    ),
                    "model.d": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
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
        "upstream_node, current_node, expected",
        [
            # Current source has no last updated at.
            (
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.SOURCE,
                ),
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                ),
                True,
            ),
            # Clean Source -> Model no config
            (
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.SOURCE,
                ),
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=10),
                ),
                False,
            ),
            # Dirty Source -> Model no config
            (
                Node(
                    freshness=Freshness.DIRTY,
                    type=NodeType.SOURCE,
                ),
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=10),
                ),
                True,
            ),
            # Clean Source -> Model with invalid config
            (
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.SOURCE,
                ),
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=10),
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
                Node(
                    freshness=Freshness.DIRTY,
                    type=NodeType.SOURCE,
                ),
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=10),
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
                Node(
                    freshness=Freshness.DIRTY,
                    type=NodeType.SOURCE,
                ),
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=20),
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
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=60),
                ),
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=20),
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
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=12),
                ),
                Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=20),
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
        self, upstream_node: Node, current_node: Node, expected: bool
    ):
        assert (
            should_mark_dirty_from_single_upstream(
                upstream_node=upstream_node,
                current_node=current_node,
            )
            is expected
        )


class TestCalculateModelsToRun:
    def test_calculate_models_to_run_propagates_dirty(self):
        # Create a simple DAG: source -> model_a -> model_b
        result = calculate_models_to_run(
            ParsedDag(
                nodes={
                    "source.test": Node(
                        freshness=Freshness.DIRTY,
                        type=NodeType.SOURCE,
                        last_updated=datetime.now(),
                    ),
                    "model.a": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                    ),
                    "model.b": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                    ),
                },
                edges=[
                    Edge(from_="source.test", to_="model.a"),
                    Edge(from_="model.a", to_="model.b"),
                ],
            )
        )

        # Both models should be dirty now
        assert result.nodes["model.a"].freshness == Freshness.DIRTY
        assert result.nodes["model.b"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_preserves_clean(self):
        result = calculate_models_to_run(
            ParsedDag(
                nodes={
                    "source.test": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.SOURCE,
                        last_updated=datetime.now() - timedelta(minutes=20),
                    ),
                    "model.a": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=datetime.now() - timedelta(minutes=10),
                    ),
                },
                edges=[Edge(from_="source.test", to_="model.a")],
            ),
        )

        # Model should remain clean
        assert result.nodes["model.a"].freshness == Freshness.CLEAN

    def test_calculate_models_to_run_respects_build_after(self):
        result = calculate_models_to_run(
            ParsedDag(
                nodes={
                    "source.test": Node(
                        freshness=Freshness.DIRTY,
                        type=NodeType.SOURCE,
                        last_updated=datetime.now(),
                    ),
                    "model.a": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        freshness_config={
                            "build_after": {"count": 1, "period": "hour"}
                        },
                        last_updated=datetime.now() - timedelta(minutes=20),
                    ),
                },
                edges=[Edge(from_="source.test", to_="model.a")],
            ),
        )

        # Model should remain clean due to build_after config
        assert result.nodes["model.a"].freshness == Freshness.CLEAN

    @pytest.mark.parametrize(
        "updates_on, result_freshness",
        [("any", Freshness.DIRTY), ("all", Freshness.CLEAN)],
    )
    def test_calculate_models_to_run_with_updates_on_any_or_all(
        self, updates_on: str, result_freshness: Freshness
    ):
        now = datetime.now()

        result = calculate_models_to_run(
            ParsedDag(
                nodes={
                    "model.a": Node(
                        freshness=Freshness.DIRTY,
                        type=NodeType.MODEL,
                        last_updated=now,
                    ),
                    "model.b": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=now - timedelta(hours=2),  # Old, shouldn't trigger
                    ),
                    "model.c": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
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
        )

        assert result.nodes["model.c"].freshness == result_freshness

    def test_calculate_models_to_run_sample_1(self):
        now = datetime.now()

        result = calculate_models_to_run(
            ParsedDag(
                nodes={
                    "source.src_orders": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.SOURCE,
                        last_updated=now - timedelta(minutes=10),
                    ),
                    "source.src_customers": Node(
                        freshness=Freshness.DIRTY,
                        type=NodeType.SOURCE,
                        last_updated=now,
                    ),
                    "model.stg_orders": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=now - timedelta(minutes=9),
                    ),
                    "model.stg_customers": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=now - timedelta(minutes=9),
                    ),
                    "model.int_orders": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=now - timedelta(minutes=8),
                    ),
                    "model.dim_customers": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=now - timedelta(minutes=8),
                    ),
                    "model.cust_orders": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
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
        )

        # Reused
        assert result.nodes["model.stg_orders"].freshness == Freshness.CLEAN
        assert result.nodes["model.int_orders"].freshness == Freshness.CLEAN

        # Rebuilt
        assert result.nodes["model.stg_customers"].freshness == Freshness.DIRTY
        assert result.nodes["model.cust_orders"].freshness == Freshness.DIRTY
        assert result.nodes["model.dim_customers"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_sample_2(self):
        now = datetime.now()

        result = calculate_models_to_run(
            ParsedDag(
                nodes={
                    "source.src_orders": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.SOURCE,
                        last_updated=now - timedelta(minutes=10),
                    ),
                    "source.src_customers": Node(
                        freshness=Freshness.DIRTY,
                        type=NodeType.SOURCE,
                        last_updated=now - timedelta(minutes=5),
                    ),
                    "model.stg_orders": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=now - timedelta(minutes=4),
                    ),
                    "model.stg_customers": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=now - timedelta(minutes=4),
                        freshness_config={
                            "build_after": {
                                "count": 7,
                                "period": "minute",
                            }
                        },
                    ),
                    "model.int_orders": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=now - timedelta(minutes=3),
                    ),
                    "model.dim_customers": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
                        last_updated=now - timedelta(minutes=3),
                    ),
                    "model.cust_orders": Node(
                        freshness=Freshness.CLEAN,
                        type=NodeType.MODEL,
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
        )

        # All reused
        assert result.nodes["model.stg_orders"].freshness == Freshness.CLEAN
        assert result.nodes["model.int_orders"].freshness == Freshness.CLEAN
        assert result.nodes["model.stg_customers"].freshness == Freshness.CLEAN
        assert result.nodes["model.cust_orders"].freshness == Freshness.CLEAN
        assert result.nodes["model.dim_customers"].freshness == Freshness.CLEAN
