from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import pytz

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


def test_registry_has_databricks_and_unknown_adapter_is_noop() -> None:
    assert "databricks" in FALLBACK_BY_ADAPTER_TYPE
    mocks = (MagicMock(), MagicMock(), MagicMock())
    assert try_registered_fallback("snowflake", *mocks) is None


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
