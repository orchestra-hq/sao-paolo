from typing import cast

import pytest

from src.orchestra_dbt.build_after import (
    parse_build_after_duration_minutes,
    parse_freshness_config,
    parse_lag_tolerance_seconds,
    propagate_freshness_config,
)
from src.orchestra_dbt.models import (
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
        with pytest.raises(TypeError, match="Invalid count"):
            parse_build_after_duration_minutes({"period": "minute", "count": "invalid"})


BA = {"period": "day", "count": 1, "updates_on": "all"}


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
                FreshnessConfig(sla_seconds=7200),
            ),
            (
                {"build_after": {"period": "day", "count": 1, "updates_on": "all"}},
                FreshnessConfig(sla_seconds=86400, updates_on="all"),
            ),
            (
                {"build_after": {"period": "minute", "count": 1, "updates_on": "foo"}},
                FreshnessConfig(sla_seconds=60),
            ),
        ],
    )
    def test_parse_freshness_config(
        self, config_on_node: dict | None, expected: FreshnessConfig
    ):
        assert parse_freshness_config({"freshness": config_on_node}) == expected

    @pytest.mark.parametrize(
        "config, expected",
        [
            ({"state": {"lag_tolerance": "4h"}}, (14400, "any", True)),
            ({"state": {"lag_tolerance": "0s"}}, (0, "any", True)),
            ({"state": {"require_fresh_data_from": "all"}}, (None, "all", False)),
            (  # state wins over build_after
                {"state": {"lag_tolerance": "30m"}, "freshness": {"build_after": BA}},
                (1800, "all", True),
            ),
            (
                {
                    "state": {"require_fresh_data_from": "any"},
                    "freshness": {"build_after": BA},
                },
                (86400, "any", False),
            ),
            (  # invalid lag_tolerance warns and falls back
                {"state": {"lag_tolerance": "soon"}, "freshness": {"build_after": BA}},
                (86400, "all", False),
            ),
        ],
    )
    def test_state_block(self, config: dict, expected: tuple):
        c = parse_freshness_config(config)
        assert (
            c.sla_seconds,
            c.updates_on,
            c.bypass_on_upstream_code_change,
        ) == expected


class TestParseLagTolerance:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("0s", 0),
            ("45m", 2700),
            ("1h30m", 5400),
            ("4 hours", 14400),
            ("2w", 1209600),
            ("500ms", 1),
            (90, 90),
            ({"count": 30, "period": "second", "updates_on": None}, 30),  # dbt-oss
        ],
    )
    def test_valid(self, value: str | int | dict, expected: int):
        assert parse_lag_tolerance_seconds(value) == expected

    @pytest.mark.parametrize("value", ["", "45", "1h30", "-5m", "4 years", True, {}])
    def test_invalid(self, value: str | int | dict):
        with pytest.raises(ValueError):
            parse_lag_tolerance_seconds(value)


class TestPropagateFreshnessConfig:
    def test_simple_chain_propagation(self):
        # A -> B -> C
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    asset_external_id="integration_account_id.model.a",
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    dbt_path="models/a.sql",
                    file_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "B": MaterialisationNode(
                    asset_external_id="integration_account_id.model.b",
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    dbt_path="models/b.sql",
                    file_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "C": MaterialisationNode(
                    asset_external_id="integration_account_id.model.c",
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    dbt_path="models/c.sql",
                    file_path="models/c.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(sla_seconds=1800),  # Has config
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
        assert updated_node_c.freshness_config.sla_seconds == 1800

        # B should inherit from C
        updated_node_b = cast(MaterialisationNode, dag.nodes["B"])
        assert updated_node_b.freshness_config.sla_seconds == 1800
        assert updated_node_b.freshness_config.inherited_from == "C"

        # A should inherit from B (which now has C's config)
        updated_node_a = cast(MaterialisationNode, dag.nodes["A"])
        assert updated_node_a.freshness_config.sla_seconds == 1800
        assert updated_node_a.freshness_config.inherited_from == "B"

    def test_branching_dag_propagation(self):
        # A -> B -> C, B -> D
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    asset_external_id="integration_account_id.model.a",
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    dbt_path="models/a.sql",
                    file_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(
                        sla_seconds=300
                    ),  # Already has config
                ),
                "B": MaterialisationNode(
                    asset_external_id="integration_account_id.model.b",
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    dbt_path="models/b.sql",
                    file_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "C": MaterialisationNode(
                    asset_external_id="integration_account_id.model.c",
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    dbt_path="models/c.sql",
                    file_path="models/c.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "D": MaterialisationNode(
                    asset_external_id="integration_account_id.model.d",
                    freshness=Freshness.CLEAN,
                    checksum="4",
                    dbt_path="models/d.sql",
                    file_path="models/d.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(sla_seconds=180),  # Has config
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
        assert updated_node_a.freshness_config.sla_seconds == 300
        assert updated_node_a.freshness_config.inherited_from is None

        # D should remain unchanged (already has config)
        updated_node_d = cast(MaterialisationNode, dag.nodes["D"])
        assert updated_node_d.freshness_config.sla_seconds == 180
        assert updated_node_d.freshness_config.inherited_from is None

        # C should continue to not have config (no children with config)
        updated_node_c = cast(MaterialisationNode, dag.nodes["C"])
        assert updated_node_c.freshness_config.sla_seconds is None
        assert updated_node_c.freshness_config.inherited_from is None

        # B should NOT get a config: one child (C) has no config, so parent stays null
        updated_node_b = cast(MaterialisationNode, dag.nodes["B"])
        assert updated_node_b.freshness_config.sla_seconds is None
        assert updated_node_b.freshness_config.inherited_from is None

    def test_multiple_children_minimum_selection(self):
        # A -> B, A -> C, where B has config 10 and C has config 5
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    asset_external_id="integration_account_id.model.a",
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    dbt_path="models/a.sql",
                    file_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "B": MaterialisationNode(
                    asset_external_id="integration_account_id.model.b",
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    dbt_path="models/b.sql",
                    file_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(sla_seconds=600),
                ),
                "C": MaterialisationNode(
                    asset_external_id="integration_account_id.model.c",
                    freshness=Freshness.CLEAN,
                    checksum="3",
                    dbt_path="models/c.sql",
                    file_path="models/c.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(sla_seconds=300),
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
        assert updated_node_a.freshness_config.sla_seconds == 300
        assert updated_node_a.freshness_config.inherited_from == "C"

    def test_existing_config_not_overwritten(self):
        """Test that nodes with existing configs are not modified."""
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    asset_external_id="integration_account_id.model.a",
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    dbt_path="models/a.sql",
                    file_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(sla_seconds=1200),  # Has config
                ),
                "B": MaterialisationNode(
                    asset_external_id="integration_account_id.model.b",
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    dbt_path="models/b.sql",
                    file_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(sla_seconds=600),  # Has config
                ),
            },
            edges=[
                Edge(from_="A", to_="B"),
            ],
        )

        propagate_freshness_config(dag)

        # Both should remain unchanged
        assert (
            cast(MaterialisationNode, dag.nodes["A"]).freshness_config.sla_seconds
            == 1200
        )
        assert (
            cast(MaterialisationNode, dag.nodes["B"]).freshness_config.sla_seconds
            == 600
        )

    def test_no_propagation_when_no_children_config(self):
        """Test that nodes without children configs don't get updated."""
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    asset_external_id="integration_account_id.model.a",
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    dbt_path="models/a.sql",
                    file_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(),  # No config
                ),
                "B": MaterialisationNode(
                    asset_external_id="integration_account_id.model.b",
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    dbt_path="models/b.sql",
                    file_path="models/b.sql",
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
            cast(MaterialisationNode, dag.nodes["A"]).freshness_config.sla_seconds
            is None
        )
        assert (
            cast(MaterialisationNode, dag.nodes["B"]).freshness_config.sla_seconds
            is None
        )

    def test_updates_on_preserved(self):
        """Test that updates_on field is not modified during propagation."""
        dag = ParsedDag(
            nodes={
                "A": MaterialisationNode(
                    asset_external_id="integration_account_id.model.a",
                    freshness=Freshness.CLEAN,
                    checksum="1",
                    dbt_path="models/a.sql",
                    file_path="models/a.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(
                        updates_on="all"
                    ),  # No sla_seconds but has updates_on
                ),
                "B": MaterialisationNode(
                    asset_external_id="integration_account_id.model.b",
                    freshness=Freshness.CLEAN,
                    checksum="2",
                    dbt_path="models/b.sql",
                    file_path="models/b.sql",
                    reason="test",
                    sources={},
                    freshness_config=FreshnessConfig(sla_seconds=1800),
                ),
            },
            edges=[
                Edge(from_="A", to_="B"),
            ],
        )

        propagate_freshness_config(dag)

        # A should get sla_seconds from B but keep updates_on="all"
        assert (
            cast(MaterialisationNode, dag.nodes["A"]).freshness_config.sla_seconds
            == 1800
        )
        assert (
            cast(MaterialisationNode, dag.nodes["A"]).freshness_config.updates_on
            == "all"
        )


def _node(sla_seconds: int | None = None) -> MaterialisationNode:
    return MaterialisationNode(
        asset_external_id="x",
        freshness=Freshness.CLEAN,
        checksum="1",
        dbt_path="x.sql",
        file_path="x.sql",
        reason="",
        sources={},
        freshness_config=FreshnessConfig(sla_seconds=sla_seconds),
    )


class TestDefaultLagTolerance:
    def test_parent_gets_min_of_explicit_child_and_default(self):
        dag = ParsedDag(
            nodes={
                "P": _node(),
                "A": _node(600),
                "B": _node(),
                "Q": _node(),
                "C": _node(7200),
            },
            edges=[
                Edge(from_="P", to_="A"),
                Edge(from_="P", to_="B"),
                Edge(from_="Q", to_="C"),
            ],
        )
        propagate_freshness_config(dag, default_sla_seconds=2700)
        config = {
            k: cast(MaterialisationNode, n).freshness_config
            for k, n in dag.nodes.items()
        }
        assert (
            config["B"].sla_seconds,
            config["P"].sla_seconds,
            config["Q"].sla_seconds,
        ) == (2700, 600, 2700)
        assert config["P"].bypass_on_upstream_code_change
        assert not config[
            "A"
        ].bypass_on_upstream_code_change  # explicit config untouched

    def test_inheriting_without_default_gets_no_bypass(self):
        dag = ParsedDag(
            nodes={"P": _node(), "C": _node(600)}, edges=[Edge(from_="P", to_="C")]
        )
        propagate_freshness_config(dag)
        config = cast(MaterialisationNode, dag.nodes["P"]).freshness_config
        assert config.sla_seconds == 600
        assert not config.bypass_on_upstream_code_change
