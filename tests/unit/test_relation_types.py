"""Tests for is_view.

A source with no `loaded_at_field`/`loaded_at_query` gets its freshness from
the relation's metadata timestamp. That timestamp only tracks data changes
for materialised relations -- for a view it reflects when the definition last
changed -- so callers use is_view to decide whether that timestamp can be
trusted at all.
"""

from types import SimpleNamespace
from typing import Any

import pytest

from src.orchestra_dbt.source_freshness.relation_types import is_view


def _node(unique_id: str = "source.p.s.tbl") -> Any:
    return SimpleNamespace(
        unique_id=unique_id, database="db", schema="public", identifier="tbl"
    )


class FakeAdapter:
    """Exercises only the generic BaseAdapter surface is_view relies on."""

    def __init__(
        self,
        found_type: Any = None,
        found: bool = True,
        raises: Exception | None = None,
    ) -> None:
        self._found_type = found_type
        self._found = found
        self._raises = raises
        self.get_relation_calls: list[dict[str, Any]] = []

        class Relation:
            @staticmethod
            def create_from(_config: Any, node: Any) -> Any:
                return SimpleNamespace(
                    database=node.database,
                    schema=node.schema,
                    identifier=node.identifier,
                )

        self.Relation = Relation

    def get_relation(self, database: str, schema: str, identifier: str) -> Any:
        if self._raises:
            raise self._raises
        self.get_relation_calls.append(
            {"database": database, "schema": schema, "identifier": identifier}
        )
        if not self._found:
            return None
        return SimpleNamespace(type=self._found_type)


class _FakeDbtEnumMember:
    """dbt's RelationType is a str Enum -- adapters hand back the member
    itself, not a bare string, and str() on it renders as "RelationType.VIEW"
    rather than "view". is_view must read .value, not str()."""

    value = "view"

    def __str__(self) -> str:
        return "RelationType.VIEW"


@pytest.mark.parametrize(
    ("adapter_value", "expected"),
    [
        ("view", True),
        ("VIEW", True),
        ("materialized_view", True),
        ("table", False),
        ("base table", False),
        ("dynamic_table", False),
        (_FakeDbtEnumMember(), True),
        (None, False),
    ],
)
def test_is_view_maps_adapter_value(adapter_value: Any, expected: bool) -> None:
    adapter = FakeAdapter(found_type=adapter_value)

    assert is_view(adapter, object(), _node()) is expected
    # Looked up by the relation's own parts, so quoting/casing policy applies.
    assert adapter.get_relation_calls == [
        {"database": "db", "schema": "public", "identifier": "tbl"}
    ]


def test_missing_relation_is_not_a_view() -> None:
    adapter = FakeAdapter(found=False)
    assert is_view(adapter, object(), _node()) is False


def test_adapter_failure_is_not_a_view() -> None:
    """A lookup failure degrades to trusting the timestamp instead of
    blocking freshness altogether."""
    adapter = FakeAdapter(raises=RuntimeError("warehouse unreachable"))
    assert is_view(adapter, object(), _node()) is False
