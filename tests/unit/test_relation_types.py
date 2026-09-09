"""Tests for get_relation_type and the RelationType enum.

A source with no `loaded_at_field`/`loaded_at_query` gets its freshness from
the relation's metadata timestamp. That timestamp only tracks data changes
for materialised relations -- for a view it reflects when the definition last
changed -- so the relation's kind decides whether it can be trusted.
"""

from types import SimpleNamespace
from typing import Any

import pytest

from src.orchestra_dbt.models import RelationType
from src.orchestra_dbt.source_freshness.relation_types import get_relation_type


def _node(unique_id: str = "source.p.s.tbl") -> Any:
    return SimpleNamespace(
        unique_id=unique_id, database="db", schema="public", identifier="tbl"
    )


class FakeAdapter:
    """Exercises only the generic BaseAdapter surface get_relation_type uses."""

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


@pytest.mark.parametrize(
    ("adapter_value", "expected"),
    [
        ("view", RelationType.VIEW),
        ("table", RelationType.TABLE),
        ("materialized_view", RelationType.MATERIALIZED_VIEW),
        # dbt's RelationType is a str enum, so real adapters hand back the
        # enum member itself rather than a bare string.
        (RelationType.VIEW, RelationType.VIEW),
        # Casing/whitespace from a warehouse response is tolerated.
        ("  VIEW  ", RelationType.VIEW),
        # Anything unrecognised degrades to None rather than raising.
        ("some_future_kind", None),
        (None, None),
    ],
)
def test_get_relation_type_maps_adapter_value(
    adapter_value: Any, expected: RelationType | None
) -> None:
    adapter = FakeAdapter(found_type=adapter_value)

    assert get_relation_type(adapter, object(), _node()) is expected
    # Looked up by the relation's own parts, so quoting/casing policy applies.
    assert adapter.get_relation_calls == [
        {"database": "db", "schema": "public", "identifier": "tbl"}
    ]


def test_missing_relation_returns_none() -> None:
    adapter = FakeAdapter(found=False)
    assert get_relation_type(adapter, object(), _node()) is None


def test_adapter_failure_returns_none_instead_of_raising() -> None:
    adapter = FakeAdapter(raises=RuntimeError("warehouse unreachable"))
    assert get_relation_type(adapter, object(), _node()) is None


@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        ("view", RelationType.VIEW),
        ("VIEW", RelationType.VIEW),
        ("table", RelationType.TABLE),
        ("materialized_view", RelationType.MATERIALIZED_VIEW),
        # A value written by a newer version must not fail an old reader.
        ("some_future_kind", None),
        ("", None),
        (None, None),
    ],
)
def test_relation_type_parse_is_lenient(
    stored: str | None, expected: RelationType | None
) -> None:
    assert RelationType.parse(stored) is expected


def test_enum_values_match_dbts_own_relation_type() -> None:
    """Our enum is the wire format for dbt's values -- keep them aligned."""
    pytest.importorskip("dbt.adapters.contracts.relation")
    from dbt.adapters.contracts.relation import RelationType as DbtRelationType

    assert {m.value for m in DbtRelationType} <= {m.value for m in RelationType}


class TestSourceRelationTypeLookup:
    """construct_dag's view of the cache: re-checked against the manifest
    each run, and keyed by the target-qualified relation name."""

    def _state(self, **cached: str):
        from src.orchestra_dbt.models import StateApiModel

        return StateApiModel(state={}, source_relation_types=dict(cached))

    def test_reads_cached_kind_by_relation_name(self) -> None:
        from src.orchestra_dbt.dag import source_relation_type

        source = {"relation_name": "DB.raw.orders"}
        state = self._state(**{"DB.raw.orders": "view"})

        assert source_relation_type(source, state) is RelationType.VIEW

    @pytest.mark.parametrize("field", ["loaded_at_field", "loaded_at_query"])
    def test_ignores_stale_kind_once_loaded_at_is_configured(self, field: str) -> None:
        """Adding loaded_at_* is the documented remedy -- it must not leave the
        source pinned dirty forever by a kind cached from before."""
        from src.orchestra_dbt.dag import source_relation_type

        source = {"relation_name": "DB.raw.orders", field: "updated_at"}
        state = self._state(**{"DB.raw.orders": "view"})

        assert source_relation_type(source, state) is None

    def test_kind_cached_for_another_target_is_not_reused(self) -> None:
        """One state file can serve several targets, where the same source id
        resolves to a different relation."""
        from src.orchestra_dbt.dag import source_relation_type

        state = self._state(**{"DEVDB.dev_raw.orders": "table"})
        prod_source = {"relation_name": "PRODDB.prod_raw.orders"}

        assert source_relation_type(prod_source, state) is None

    def test_unknown_source_has_no_relation_type(self) -> None:
        from src.orchestra_dbt.dag import source_relation_type

        assert source_relation_type({}, self._state()) is None
        assert (
            source_relation_type({"relation_name": "DB.raw.x"}, self._state()) is None
        )
