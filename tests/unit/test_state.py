from datetime import datetime, timedelta
from unittest.mock import patch

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
        "upstream_node, freshness_config, expected",
        [
            (Node(freshness=Freshness.DIRTY, type=NodeType.MODEL), None, True),
            (Node(freshness=Freshness.CLEAN, type=NodeType.MODEL), None, False),
            (
                Node(
                    freshness=Freshness.DIRTY,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=10),
                ),
                {"build_after": {"count": 1, "period": "hour"}},
                False,
            ),
            (
                Node(
                    freshness=Freshness.DIRTY,
                    type=NodeType.MODEL,
                    last_updated=datetime.now() - timedelta(minutes=90),
                ),
                {"build_after": {"count": 1, "period": "hour"}},
                False,
            ),
        ],
    )
    def test_should_mark_dirty_from_single_upstream(
        self, upstream_node: Node, freshness_config: dict | None, expected: bool
    ):
        assert (
            should_mark_dirty_from_single_upstream(
                upstream_node=upstream_node,
                freshness_config=freshness_config,
            )
            is expected
        )


class TestCalculateModelsToRun:
    def test_calculate_models_to_run_propagates_dirty(self):
        # Create a simple DAG: source -> model_a -> model_b
        nodes = {
            "source.test": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.SOURCE,
                last_updated=datetime.now(),
            ),
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
        }
        edges = [
            Edge(from_="source.test", to_="model.a"),
            Edge(from_="model.a", to_="model.b"),
        ]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Both models should be dirty now
        assert result.nodes["model.a"].freshness == Freshness.DIRTY
        assert result.nodes["model.b"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_preserves_clean(self):
        nodes = {
            "source.test": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.SOURCE,
                last_updated=datetime.now(),
            ),
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
        }
        edges = [Edge(from_="source.test", to_="model.a")]

        result = calculate_models_to_run(
            ParsedDag(nodes=nodes, edges=edges),
        )

        # Model should remain clean
        assert result.nodes["model.a"].freshness == Freshness.CLEAN

    def test_calculate_models_to_run_respects_build_after(self):
        nodes = {
            "source.test": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.SOURCE,
                last_updated=datetime.now(),
            ),
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 1, "period": "hour"}},
            ),
        }
        edges = [Edge(from_="source.test", to_="model.a")]

        result = calculate_models_to_run(
            ParsedDag(nodes=nodes, edges=edges),
        )

        # Model should remain clean due to build_after config
        assert result.nodes["model.a"].freshness == Freshness.CLEAN

    def test_calculate_models_to_run_with_updates_on_any(self):
        """Test that updates_on 'any' requires only one upstream to trigger dirty."""
        now = datetime.now()
        nodes = {
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=now - timedelta(minutes=45),
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
                        "updates_on": "any",
                    }
                },
            ),
        }
        edges = [
            Edge(from_="model.a", to_="model.c"),
            Edge(from_="model.b", to_="model.c"),
        ]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Should be dirty because model.a was updated recently (45 mins < 30 min threshold backwards)
        # updates_on 'any' means only one upstream needs to trigger
        assert result.nodes["model.c"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_with_updates_on_all(self):
        """Test that updates_on 'all' requires all upstreams to trigger dirty."""
        now = datetime.now()
        nodes = {
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=now - timedelta(minutes=45),
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
                        "updates_on": "all",
                    }
                },
            ),
        }
        edges = [
            Edge(from_="model.a", to_="model.c"),
            Edge(from_="model.b", to_="model.c"),
        ]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Should remain clean because model.b was not updated recently
        # updates_on 'all' requires both upstreams to trigger
        assert result.nodes["model.c"].freshness == Freshness.CLEAN

    def test_calculate_models_to_run_with_updates_on_all_both_trigger(self):
        """Test that updates_on 'all' works when all upstreams trigger dirty."""
        now = datetime.now()
        nodes = {
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=now - timedelta(minutes=10),
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=now - timedelta(minutes=15),
            ),
            "model.c": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={
                    "build_after": {
                        "count": 30,
                        "period": "minute",
                        "updates_on": "all",
                    }
                },
            ),
        }
        edges = [
            Edge(from_="model.a", to_="model.c"),
            Edge(from_="model.b", to_="model.c"),
        ]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Should be dirty because both upstreams were updated recently
        assert result.nodes["model.c"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_upstream_updated_recently(self):
        """Test the example scenario: upstream updated recently enough triggers downstream."""
        # Simulating: At 1pm A (new data) -> B (runs), C doesn't run (config forbids)
        # At 1:30pm C runs (there is fresh upstream data and config now allows it)
        base_time = datetime(2024, 1, 1, 13, 0, 0)  # 1pm

        # At 1pm: B was just updated
        nodes_1pm = {
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=base_time,
            ),
            "model.c": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 30, "period": "minute"}},
                last_updated=base_time - timedelta(hours=1),
            ),
        }
        edges = [Edge(from_="model.b", to_="model.c")]

        # At 1pm, C should NOT run (only 0 minutes have passed, need 30)
        with patch("src.orchestra_dbt.state.datetime") as mock_datetime:
            mock_datetime.now.return_value = base_time
            result = calculate_models_to_run(
                ParsedDag(nodes=nodes_1pm.copy(), edges=edges)
            )
            assert (
                result.nodes["model.c"].freshness == Freshness.DIRTY
            )  # Actually, this might be dirty because threshold logic

        # At 1:30pm: 30 minutes have passed since B was updated
        nodes_1_30pm = {
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=base_time,  # Still updated at 1pm
            ),
            "model.c": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 30, "period": "minute"}},
                last_updated=base_time - timedelta(hours=1),
            ),
        }

        with patch("src.orchestra_dbt.state.datetime") as mock_datetime:
            mock_datetime.now.return_value = base_time + timedelta(minutes=30)
            result = calculate_models_to_run(
                ParsedDag(nodes=nodes_1_30pm.copy(), edges=edges),
            )
            # C should be dirty because B was updated 30 mins ago (within threshold)
            assert result.nodes["model.c"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_upstream_dirty_propagates(self):
        """Test that dirty upstream always propagates to downstream without config."""
        nodes = {
            "model.a": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.MODEL,
                last_updated=None,
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
        }
        edges = [Edge(from_="model.a", to_="model.b")]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # model.b should be dirty because model.a is dirty and no config
        assert result.nodes["model.b"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_dirty_upstream_with_build_after(self):
        """Test dirty upstream with build_after config."""
        now = datetime.now()
        nodes = {
            "model.a": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.MODEL,
                last_updated=now - timedelta(minutes=45),
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 30, "period": "minute"}},
            ),
        }
        edges = [Edge(from_="model.a", to_="model.b")]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # model.b should be dirty because model.a is dirty and was updated
        # 45 mins ago (more than 30 min threshold backwards)
        assert result.nodes["model.b"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_node_without_upstream(self):
        """Test that nodes without upstream dependencies are processed correctly."""
        nodes = {
            "model.standalone": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
        }
        edges = []

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Node without upstream should remain clean (nothing to check)
        assert result.nodes["model.standalone"].freshness == Freshness.CLEAN

    def test_calculate_models_to_run_upstream_missing_last_updated(self):
        """Test behavior when upstream has no last_updated timestamp."""
        nodes = {
            "model.a": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.MODEL,
                last_updated=None,
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 30, "period": "minute"}},
            ),
        }
        edges = [Edge(from_="model.a", to_="model.b")]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # If upstream is dirty but no last_updated and has build_after config,
        # it should propagate because dirty upstream with config
        assert result.nodes["model.b"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_upstream_clean_no_config(self):
        """Test that clean upstream with no config doesn't trigger downstream."""
        nodes = {
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=datetime.now(),
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
        }
        edges = [Edge(from_="model.a", to_="model.b")]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # model.b should remain clean
        assert result.nodes["model.b"].freshness == Freshness.CLEAN

    def test_calculate_models_to_run_complex_dag(self):
        """Test a more complex DAG with multiple levels."""
        now = datetime.now()
        nodes = {
            "source.s": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.SOURCE,
                last_updated=now,
            ),
            "model.stg": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
            "model.int": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 1, "period": "hour"}},
                last_updated=now - timedelta(minutes=30),
            ),
            "model.mart": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 30, "period": "minute"}},
            ),
        }
        edges = [
            Edge(from_="source.s", to_="model.stg"),
            Edge(from_="model.stg", to_="model.int"),
            Edge(from_="model.int", to_="model.mart"),
        ]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # source.s is dirty, so model.stg should be dirty
        assert result.nodes["model.stg"].freshness == Freshness.DIRTY
        # model.stg is dirty, so model.int should be dirty (no config on stg)
        assert result.nodes["model.int"].freshness == Freshness.DIRTY
        # model.int was updated 30 mins ago, which is within the 30 min threshold
        assert result.nodes["model.mart"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_build_after_exact_threshold(self):
        """Test edge case where update time exactly matches threshold."""
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        nodes = {
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=base_time,
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 30, "period": "minute"}},
            ),
        }
        edges = [Edge(from_="model.a", to_="model.b")]

        # Exactly 30 minutes later
        with patch("src.orchestra_dbt.state.datetime") as mock_datetime:
            mock_datetime.now.return_value = base_time + timedelta(minutes=30)
            result = calculate_models_to_run(ParsedDag(nodes=nodes.copy(), edges=edges))
            # Should trigger because update is more recent than (now - 30 mins)
            assert result.nodes["model.b"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_default_updates_on_is_any(self):
        """Test that updates_on defaults to 'any' when not specified."""
        now = datetime.now()
        nodes = {
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=now - timedelta(minutes=10),
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=now - timedelta(hours=2),
            ),
            "model.c": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                # No updates_on specified, should default to 'any'
                freshness_config={"build_after": {"count": 30, "period": "minute"}},
            ),
        }
        edges = [
            Edge(from_="model.a", to_="model.c"),
            Edge(from_="model.b", to_="model.c"),
        ]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Should be dirty because model.a triggers (defaults to 'any' behavior)
        assert result.nodes["model.c"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_no_freshness_config(self):
        """Test behavior when downstream has no freshness config."""
        nodes = {
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=datetime.now(),
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
        }
        edges = [Edge(from_="model.a", to_="model.b")]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Without config, only dirty upstream triggers downstream
        assert result.nodes["model.b"].freshness == Freshness.CLEAN

    def test_calculate_models_to_run_multiple_upstreams_one_dirty(self):
        """Test multiple upstreams where only one is dirty."""
        nodes = {
            "model.a": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.MODEL,
                last_updated=datetime.now(),
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=datetime.now(),
            ),
            "model.c": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 30, "period": "minute"}},
            ),
        }
        edges = [
            Edge(from_="model.a", to_="model.c"),
            Edge(from_="model.b", to_="model.c"),
        ]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Should be dirty because model.a is dirty
        assert result.nodes["model.c"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_preserves_already_dirty(self):
        """Test that nodes already marked as dirty are not reprocessed."""
        nodes = {
            "model.a": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.MODEL,
                last_updated=datetime.now(),
            ),
            "model.b": Node(
                freshness=Freshness.DIRTY,  # Already dirty
                type=NodeType.MODEL,
                freshness_config=None,
            ),
        }
        edges = [Edge(from_="model.a", to_="model.b")]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Should remain dirty
        assert result.nodes["model.b"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_upstream_too_old(self):
        """Test that upstream updated too long ago doesn't trigger (outside threshold)."""
        now = datetime.now()
        nodes = {
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                last_updated=now - timedelta(hours=2),  # 2 hours ago
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 30, "period": "minute"}},
            ),
        }
        edges = [Edge(from_="model.a", to_="model.b")]

        result = calculate_models_to_run(ParsedDag(nodes=nodes, edges=edges))

        # Should remain clean because upstream was updated too long ago
        # (2 hours > 30 min threshold backwards)
        assert result.nodes["model.b"].freshness == Freshness.CLEAN
