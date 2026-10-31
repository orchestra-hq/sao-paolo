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
    SourceNode,
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
                {"freshness": {"build_after": {"period": "hour", "count": 2}}},
                FreshnessConfig(sla_seconds=7200, source="build_after"),
            ),
            (
                {
                    "freshness": {
                        "build_after": {
                            "period": "day",
                            "count": 1,
                            "updates_on": "all",
                        }
                    }
                },
                FreshnessConfig(
                    sla_seconds=86400, updates_on="all", source="build_after"
                ),
            ),
            (
                {
                    "freshness": {
                        "build_after": {
                            "period": "minute",
                            "count": 1,
                            "updates_on": "foo",
                        }
                    }
                },
                FreshnessConfig(sla_seconds=60, source="build_after"),
            ),
            (
                {"freshness": None, "state": None},
                FreshnessConfig(),
            ),
            # dbt State `state` block
            (
                {"state": {"lag_tolerance": "4h"}},
                FreshnessConfig(
                    sla_seconds=14400,
                    source="lag_tolerance",
                    bypass_on_upstream_code_change=True,
                ),
            ),
            (
                {"state": {"lag_tolerance": "0s", "require_fresh_data_from": "ALL"}},
                FreshnessConfig(
                    sla_seconds=0,
                    updates_on="all",
                    source="lag_tolerance",
                    bypass_on_upstream_code_change=True,
                ),
            ),
            (
                {"state": {"require_fresh_data_from": "all"}},
                FreshnessConfig(updates_on="all"),
            ),
            # object form allowed by dbt's JSON schema
            (
                {
                    "state": {
                        "lag_tolerance": {
                            "count": 2,
                            "period": "hour",
                            "updates_on": "all",
                        }
                    }
                },
                FreshnessConfig(
                    sla_seconds=7200,
                    updates_on="all",
                    source="lag_tolerance",
                    bypass_on_upstream_code_change=True,
                ),
            ),
            # dbt-oss manifest: updates_on None in the object falls through
            (
                {
                    "state": {
                        "lag_tolerance": {
                            "count": 30,
                            "period": "second",
                            "updates_on": None,
                        }
                    },
                    "freshness": {
                        "build_after": {
                            "period": "hour",
                            "count": 1,
                            "updates_on": "all",
                        }
                    },
                },
                FreshnessConfig(
                    sla_seconds=30,
                    updates_on="all",
                    source="lag_tolerance",
                    bypass_on_upstream_code_change=True,
                ),
            ),
            # state block wins over build_after
            (
                {
                    "state": {"lag_tolerance": "30m", "require_fresh_data_from": "any"},
                    "freshness": {
                        "build_after": {
                            "period": "day",
                            "count": 1,
                            "updates_on": "all",
                        }
                    },
                },
                FreshnessConfig(
                    sla_seconds=1800,
                    source="lag_tolerance",
                    bypass_on_upstream_code_change=True,
                ),
            ),
            # each key falls back independently
            (
                {
                    "state": {"require_fresh_data_from": "all"},
                    "freshness": {"build_after": {"period": "hour", "count": 1}},
                },
                FreshnessConfig(
                    sla_seconds=3600, updates_on="all", source="build_after"
                ),
            ),
            (
                {
                    "state": {"lag_tolerance": "1d"},
                    "freshness": {
                        "build_after": {
                            "period": "hour",
                            "count": 1,
                            "updates_on": "all",
                        }
                    },
                },
                FreshnessConfig(
                    sla_seconds=86400,
                    updates_on="all",
                    source="lag_tolerance",
                    bypass_on_upstream_code_change=True,
                ),
            ),
        ],
    )
    def test_parse_freshness_config(
        self, config_on_node: dict | None, expected: FreshnessConfig
    ):
        assert parse_freshness_config(config_on_node) == expected

    def test_invalid_lag_tolerance_warns_and_falls_back(
        self, capsys: pytest.CaptureFixture[str]
    ):
        config = {
            "state": {"lag_tolerance": "soon"},
            "freshness": {"build_after": {"period": "hour", "count": 1}},
        }
        assert parse_freshness_config(config, node_id="model.a") == FreshnessConfig(
            sla_seconds=3600, source="build_after"
        )
        assert (
            parse_freshness_config(
                {"state": {"lag_tolerance": "soon"}}, node_id="model.a"
            )
            == FreshnessConfig()
        )
        assert "model.a" in capsys.readouterr().out


class TestParseLagTolerance:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("0s", 0),
            ("30s", 30),
            ("45m", 2700),
            ("4h", 14400),
            ("1d", 86400),
            ("2w", 1209600),
            (" 7D ", 604800),
            ("10 m", 600),
            ("1h30m", 5400),
            ("1h 30m", 5400),
            ("4 hours", 14400),
            ("120sec", 120),
            ("1500ms", 2),
            ("500ms", 1),
            ("1s500ms", 2),
            ("3 days", 259200),
            (90, 90),
            # dbt-oss manifest form
            ({"count": 30, "period": "second", "updates_on": None}, 30),
            ({"count": 0, "period": "minute", "updates_on": None}, 0),
            ({"count": 90, "period": "minute"}, 5400),
            ({"count": 14, "period": "day"}, 1209600),
        ],
    )
    def test_valid(self, value: str | int | dict, expected: int):
        assert parse_lag_tolerance_seconds(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "m",
            "45",
            "45x",
            "4 fortnights",
            "1h30",
            "-5m",
            "1.5h",
            -1,
            True,
            {"count": 1},
            {"count": "1", "period": "hour"},
            {"count": 1, "period": "year"},
            {"count": -1, "period": "hour"},
        ],
    )
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


def _node(node_id: str, freshness_config: FreshnessConfig) -> MaterialisationNode:
    return MaterialisationNode(
        asset_external_id=node_id,
        freshness=Freshness.CLEAN,
        checksum="1",
        dbt_path=f"models/{node_id}.sql",
        file_path=f"models/{node_id}.sql",
        reason="",
        sources={},
        freshness_config=freshness_config,
    )


def _config(dag: ParsedDag, node_id: str) -> FreshnessConfig:
    return cast(MaterialisationNode, dag.nodes[node_id]).freshness_config


class TestLagTolerancePropagation:
    def test_parent_inherits_source_of_min_child(self):
        dag = ParsedDag(
            nodes={
                "A": _node("A", FreshnessConfig()),
                "B": _node(
                    "B", FreshnessConfig(sla_seconds=3600, source="build_after")
                ),
                "C": _node(
                    "C",
                    FreshnessConfig(
                        sla_seconds=600,
                        source="lag_tolerance",
                        bypass_on_upstream_code_change=True,
                    ),
                ),
            },
            edges=[Edge(from_="A", to_="B"), Edge(from_="A", to_="C")],
        )
        propagate_freshness_config(dag)
        assert _config(dag, "A") == FreshnessConfig(
            sla_seconds=600, source="lag_tolerance", inherited_from="C"
        )

    def test_zero_lag_tolerance_is_defined_and_propagates(self):
        dag = ParsedDag(
            nodes={
                "A": _node("A", FreshnessConfig()),
                "B": _node(
                    "B",
                    FreshnessConfig(
                        sla_seconds=0,
                        source="lag_tolerance",
                        bypass_on_upstream_code_change=True,
                    ),
                ),
                "C": _node(
                    "C",
                    FreshnessConfig(
                        sla_seconds=0,
                        source="lag_tolerance",
                        bypass_on_upstream_code_change=True,
                    ),
                ),
            },
            edges=[Edge(from_="A", to_="B"), Edge(from_="B", to_="C")],
        )
        propagate_freshness_config(dag)
        assert _config(dag, "B").sla_seconds == 0
        assert _config(dag, "B").inherited_from is None
        assert _config(dag, "A").sla_seconds == 0
        assert _config(dag, "A").inherited_from == "B"


class TestDefaultLagTolerance:
    def test_parent_inherits_explicit_child_below_default(self):
        """P -> A (10m) and P -> B (unset): B gets the 45m default, P inherits 10m."""
        dag = ParsedDag(
            nodes={
                "P": _node("P", FreshnessConfig()),
                "A": _node(
                    "A", FreshnessConfig(sla_seconds=600, source="lag_tolerance")
                ),
                "B": _node("B", FreshnessConfig()),
            },
            edges=[Edge(from_="P", to_="A"), Edge(from_="P", to_="B")],
        )
        propagate_freshness_config(dag, default_sla_seconds=2700)
        assert _config(dag, "B") == FreshnessConfig(
            sla_seconds=2700,
            source="lag_tolerance",
            bypass_on_upstream_code_change=True,
        )
        assert _config(dag, "P") == FreshnessConfig(
            sla_seconds=600,
            source="lag_tolerance",
            inherited_from="A",
            bypass_on_upstream_code_change=True,
        )

    def test_explicit_configs_untouched(self):
        dag = ParsedDag(
            nodes={
                "source.s": SourceNode(),
                "A": _node("A", FreshnessConfig(updates_on="all")),
                "B": _node("B", FreshnessConfig(sla_seconds=60, source="build_after")),
            },
            edges=[Edge(from_="source.s", to_="A")],
        )
        propagate_freshness_config(dag, default_sla_seconds=2700)
        assert _config(dag, "A") == FreshnessConfig(
            sla_seconds=2700,
            updates_on="all",
            source="lag_tolerance",
            bypass_on_upstream_code_change=True,
        )
        assert _config(dag, "B") == FreshnessConfig(
            sla_seconds=60, source="build_after"
        )
        assert dag.nodes["source.s"] == SourceNode()

    def test_without_default_inheritance_does_not_grant_bypass(self):
        dag = ParsedDag(
            nodes={
                "P": _node("P", FreshnessConfig()),
                "C": _node(
                    "C",
                    FreshnessConfig(
                        sla_seconds=600,
                        source="lag_tolerance",
                        bypass_on_upstream_code_change=True,
                    ),
                ),
            },
            edges=[Edge(from_="P", to_="C")],
        )
        propagate_freshness_config(dag)
        assert _config(dag, "P").sla_seconds == 600
        assert _config(dag, "P").bypass_on_upstream_code_change is False
