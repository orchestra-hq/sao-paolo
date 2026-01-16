from typing import cast

import pytest

from orchestra_dbt.build_after import (
    parse_build_after_duration_minutes,
    parse_freshness_config,
    propagate_freshness_config,
)
from orchestra_dbt.models import (
    Edge,
    Freshness,
    FreshnessConfig,
    MaterialisationNode,
    ParsedDag,
)


class TestBuildAfterDurationMinutes:
    @pytest.mark.parametrize(
        "period, count, expected",
        [("minute", 30, 30), ("hour", 2, 120), ("day", 1, 1440), ("minute", 0, 0)],
    )
    def test_build_after_duration_minutes(self, period: str, count: int, expected: int):
        assert (
            parse_build_after_duration_minutes({"period": period, "count": count})
            == expected
        )

    def test_build_after_duration_minutes_invalid_period(self):
        with pytest.raises(ValueError, match="Invalid period"):
            parse_build_after_duration_minutes({"period": "invalid", "count": 1})

    def test_build_after_duration_minutes_invalid_count(self):
        with pytest.raises(ValueError, match="Invalid count"):
            parse_build_after_duration_minutes({"period": "minute", "count": "invalid"})


class TestParseFreshnessConfig:
    @pytest.mark.parametrize(
        "config_on_node, expected",
        [
            (
                None,
                FreshnessConfig(),
            ),
            (
                {},
                FreshnessConfig(),
            ),
            (
                {"foo": "bar"},
                FreshnessConfig(),
            ),
            (
                {"build_after": {"period": "hour", "count": 2}},
                FreshnessConfig(minutes_sla=120),
            ),
            (
                {"build_after": {"period": "day", "count": 1, "updates_on": "all"}},
                FreshnessConfig(minutes_sla=1440, updates_on="all"),
            ),
            (
                {"build_after": {"period": "minute", "count": 1, "updates_on": "foo"}},
                FreshnessConfig(minutes_sla=1),
            ),
        ],
    )
    def test_parse_freshness_config(
        self, config_on_node: dict | None, expected: FreshnessConfig
    ):
        assert parse_freshness_config(config_on_node) == expected


class TestPropagateFreshnessConfig:
    def test_simple_chain_propagation(self):
        # A -> B -> C
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/a.sql",
                    sql_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "B": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/b.sql",
                    sql_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "C": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    node_path="models/c.sql",
                    sql_path="models/c.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(minutes_sla=30),  # Has config
                ),
            },
            edges=[
                Edge(from_="A", to_="B"),
                Edge(from_="B", to_="C"),
            ],
        )

        propagate_freshness_config(dag)

        # C should remain unchanged (already has config)
        updated_node_c = cast(MaterialisationNode, dag.nodes["C"])
        assert updated_node_c.freshness_config.minutes_sla == 30

        # B should inherit from C
        updated_node_b = cast(MaterialisationNode, dag.nodes["B"])
        assert updated_node_b.freshness_config.minutes_sla == 30
        assert updated_node_b.freshness_config.inherited_from == "C"

        # A should inherit from B (which now has C's config)
        updated_node_a = cast(MaterialisationNode, dag.nodes["A"])
        assert updated_node_a.freshness_config.minutes_sla == 30
        assert updated_node_a.freshness_config.inherited_from == "B"

    def test_branching_dag_propagation(self):
        # A -> B -> C, B -> D
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/a.sql",
                    sql_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(
                        minutes_sla=5
                    ),  # Already has config
                ),
                "B": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/b.sql",
                    sql_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "C": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    node_path="models/c.sql",
                    sql_path="models/c.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "D": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="4",
                    node_path="models/d.sql",
                    sql_path="models/d.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(minutes_sla=3),  # Has config
                ),
            },
            edges=[
                Edge(from_="A", to_="B"),
                Edge(from_="B", to_="C"),
                Edge(from_="B", to_="D"),
            ],
        )

        propagate_freshness_config(dag)

        # A should remain unchanged (already has config)
        updated_node_a = cast(MaterialisationNode, dag.nodes["A"])
        assert updated_node_a.freshness_config.minutes_sla == 5
        assert updated_node_a.freshness_config.inherited_from is None

        # D should remain unchanged (already has config)
        updated_node_d = cast(MaterialisationNode, dag.nodes["D"])
        assert updated_node_d.freshness_config.minutes_sla == 3
        assert updated_node_d.freshness_config.inherited_from is None

        # C should continue to not have config (no children with config)
        updated_node_c = cast(MaterialisationNode, dag.nodes["C"])
        assert updated_node_c.freshness_config.minutes_sla is None
        assert updated_node_c.freshness_config.inherited_from is None

        # B should inherit from D (smallest of children's configs)
        updated_node_b = cast(MaterialisationNode, dag.nodes["B"])
        assert updated_node_b.freshness_config.minutes_sla == 3
        assert updated_node_b.freshness_config.inherited_from == "D"

    def test_multiple_children_minimum_selection(self):
        # A -> B, A -> C, where B has config 10 and C has config 5
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/a.sql",
                    sql_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "B": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/b.sql",
                    sql_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(minutes_sla=10),
                ),
                "C": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    node_path="models/c.sql",
                    sql_path="models/c.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(minutes_sla=5),
                ),
            },
            edges=[
                Edge(from_="A", to_="B"),
                Edge(from_="A", to_="C"),
            ],
        )

        propagate_freshness_config(dag)

        # A should get the minimum of B and C's configs
        updated_node_a = cast(MaterialisationNode, dag.nodes["A"])
        assert updated_node_a.freshness_config.minutes_sla == 5
        assert updated_node_a.freshness_config.inherited_from == "C"

    def test_existing_config_not_overwritten(self):
        """Test that nodes with existing configs are not modified."""
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/a.sql",
                    sql_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(minutes_sla=20),  # Has config
                ),
                "B": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/b.sql",
                    sql_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(minutes_sla=10),  # Has config
                ),
            },
            edges=[
                Edge(from_="A", to_="B"),
            ],
        )

        propagate_freshness_config(dag)

        # Both should remain unchanged
        assert (
            cast(MaterialisationNode, dag.nodes["A"]).freshness_config.minutes_sla == 20
        )
        assert (
            cast(MaterialisationNode, dag.nodes["B"]).freshness_config.minutes_sla == 10
        )

    def test_no_propagation_when_no_children_config(self):
        """Test that nodes without children configs don't get updated."""
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/a.sql",
                    sql_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "B": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/b.sql",
                    sql_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
            },
            edges=[
                Edge(from_="A", to_="B"),
            ],
        )

        propagate_freshness_config(dag)

        # Neither should have config since neither started with one
        assert (
            cast(MaterialisationNode, dag.nodes["A"]).freshness_config.minutes_sla
            is None
        )
        assert (
            cast(MaterialisationNode, dag.nodes["B"]).freshness_config.minutes_sla
            is None
        )

    def test_updates_on_preserved(self):
        """Test that updates_on field is not modified during propagation."""
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    node_path="models/a.sql",
                    sql_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(
                        updates_on="all"
                    ),  # No minutes_sla but has updates_on
                ),
                "B": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    node_path="models/b.sql",
                    sql_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(minutes_sla=30),
                ),
            },
            edges=[
                Edge(from_="A", to_="B"),
            ],
        )

        propagate_freshness_config(dag)

        # A should get minutes_sla from B but keep updates_on="all"
        assert (
            cast(MaterialisationNode, dag.nodes["A"]).freshness_config.minutes_sla == 30
        )
        assert (
            cast(MaterialisationNode, dag.nodes["A"]).freshness_config.updates_on
            == "all"
        )
