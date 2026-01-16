import pytest

from orchestra_dbt.build_after import (
    parse_build_after_duration_minutes,
    parse_freshness_config,
)
from orchestra_dbt.models import FreshnessConfig


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
