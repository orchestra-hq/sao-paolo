from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import pytz

from src.orchestra_dbt.source_freshness.fallbacks import snowflake as snowflake_fallback
from src.orchestra_dbt.source_freshness.fallbacks.common import (
    build_source_freshness_result_from_loaded_at,
    parse_query_timestamp_cell,
)
from src.orchestra_dbt.source_freshness.fallbacks.registry import (
    FALLBACK_BY_ADAPTER_TYPE,
    loaded_at_fields_unset,
    try_registered_fallback,
)


def test_parse_query_timestamp_cell_datetimes() -> None:
    aware = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    assert parse_query_timestamp_cell(aware) is aware
    naive = datetime(2024, 6, 1, 10, 0, 0)
    out = parse_query_timestamp_cell(naive)
    assert out.tzinfo == pytz.UTC and out.replace(tzinfo=None) == naive


def test_parse_query_timestamp_cell_iso_strings() -> None:
    zulu = parse_query_timestamp_cell("2024-06-01T10:00:00Z")
    assert zulu.tzinfo == timezone.utc and zulu.hour == 10
    naive_str = parse_query_timestamp_cell("2024-06-01T10:00:00")
    assert naive_str.tzinfo == pytz.UTC


@pytest.mark.parametrize(
    ("value", "match"),
    [
        ("not-a-date", "Unable to parse"),
        (12345, "Unexpected timestamp type"),
    ],
)
def test_parse_query_timestamp_cell_rejects_invalid(value: object, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        parse_query_timestamp_cell(value)


def test_registry_has_databricks_snowflake_and_unknown_adapter_is_noop() -> None:
    assert "databricks" in FALLBACK_BY_ADAPTER_TYPE
    assert "snowflake" in FALLBACK_BY_ADAPTER_TYPE
    mocks = (MagicMock(), MagicMock(), MagicMock())
    assert try_registered_fallback("unknown", *mocks) is None


@pytest.mark.parametrize(
    ("query", "field", "expected"),
    [
        (None, None, True),
        ("select 1", None, False),
        (None, "updated_at", False),
    ],
)
def test_loaded_at_fields_unset(
    query: str | None, field: str | None, expected: bool
) -> None:
    node = SimpleNamespace(loaded_at_query=query, loaded_at_field=field)
    assert loaded_at_fields_unset(node) is expected


def test_build_source_freshness_result_from_loaded_at_no_freshness_config() -> None:
    pytest.importorskip("dbt.artifacts")
    from dbt.artifacts.schemas.results import FreshnessStatus

    node = SimpleNamespace(freshness=None, unique_id="source.x.y")
    dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    result = build_source_freshness_result_from_loaded_at(
        max_loaded_at=dt,
        compiled_node=node,
        adapter_response=None,
    )
    assert result.status == FreshnessStatus.Pass
    assert result.max_loaded_at == dt
    assert result.node is node


def test_snowflake_fallback_uses_last_altered_metadata(monkeypatch) -> None:
    runner = MagicMock()
    relation = SimpleNamespace(
        database="analytics",
        schema="public",
        identifier="orders",
        quote_policy={},
    )
    runner.adapter.Relation.create_from.return_value = relation
    runner.adapter.connection_named.return_value.__enter__.return_value = None
    runner.adapter.connection_named.return_value.__exit__.return_value = None

    adapter_response = MagicMock()
    timestamp = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    runner.adapter.execute.return_value = (
        adapter_response,
        SimpleNamespace(rows=[(timestamp,)]),
    )
    expected_result = object()
    build_result = MagicMock(return_value=expected_result)
    monkeypatch.setattr(
        snowflake_fallback,
        "build_source_freshness_result_from_loaded_at",
        build_result,
    )

    compiled_node = SimpleNamespace(unique_id="source.test.orders")

    result = snowflake_fallback.try_snowflake_fallback(
        runner, compiled_node, MagicMock()
    )

    assert result is expected_result
    runner.adapter.execute.assert_called_once_with(
        sql=(
            "SELECT LAST_ALTERED "
            "FROM ANALYTICS.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'PUBLIC' "
            "AND TABLE_NAME = 'ORDERS' "
            "LIMIT 1"
        ),
        auto_begin=False,
        fetch=True,
    )
    build_result.assert_called_once_with(
        max_loaded_at=timestamp,
        compiled_node=compiled_node,
        adapter_response=adapter_response,
    )
