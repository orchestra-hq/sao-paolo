import threading
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.orchestra_dbt.warehouse.existence import (
    RelationExistence,
    build_schema_groups,
    check_relations_exist,
    normalise_part,
)
from src.orchestra_dbt.warehouse.registry import (
    EXISTENCE_CHECK_UNSUPPORTED,
    EXISTENCE_OVERRIDE_BY_ADAPTER_TYPE,
    existence_check_supported,
    try_registered_override,
)


class FakeRelation:
    """Stands in for a dbt BaseRelation."""

    def __init__(
        self, database: str | None, schema: str | None, identifier: str | None
    ) -> None:
        self.database = database
        self.schema = schema
        self.identifier = identifier

    def without_identifier(self) -> "FakeRelation":
        return FakeRelation(self.database, self.schema, None)

    def __repr__(self) -> str:
        return f"{self.database}.{self.schema}"


def make_manifest(nodes: dict[str, tuple[str, str, str]]) -> SimpleNamespace:
    """nodes: {unique_id: (database, schema, alias)}"""
    return SimpleNamespace(
        nodes={
            unique_id: {"database": db, "schema": schema, "alias": alias}
            for unique_id, (db, schema, alias) in nodes.items()
        }
    )


def make_adapter(
    listings: dict[tuple[str, str], list[str] | Exception],
    adapter_type: str = "postgres",
) -> tuple[MagicMock, list[tuple[str, str]]]:
    calls: list[tuple[str, str]] = []
    adapter = MagicMock()
    adapter.type.return_value = adapter_type
    adapter.Relation.create_from.side_effect = lambda quoting, relation_config: (
        FakeRelation(
            relation_config["database"],
            relation_config["schema"],
            relation_config["alias"],
        )
    )

    def list_relations(schema_relation: FakeRelation) -> list[FakeRelation]:
        key = (
            normalise_part(schema_relation.database),
            normalise_part(schema_relation.schema),
        )
        calls.append(key)
        outcome = listings[key]
        if isinstance(outcome, Exception):
            raise outcome
        return [
            FakeRelation(schema_relation.database, schema_relation.schema, identifier)
            for identifier in outcome
        ]

    adapter.list_relations_without_caching.side_effect = list_relations
    return adapter, calls


class TestNormalisePart:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("MY_MODEL", "my_model"),
            ('"MyModel"', "mymodel"),
            ("`my_model`", "my_model"),
            ("[my_model]", "my_model"),
            ("  my_model  ", "my_model"),
            (None, ""),
        ],
    )
    def test_normalisation(self, value: str | None, expected: str) -> None:
        assert normalise_part(value) == expected


class TestBuildSchemaGroups:
    def test_groups_by_schema_and_skips_unusable_nodes(self) -> None:
        manifest = make_manifest(
            {
                "model.p.a": ("db", "analytics", "a"),
                "model.p.b": ("db", "analytics", "b"),
                "model.p.c": ("db", "staging", "c"),
                "model.p.no_schema": ("db", "", "d"),
                "model.p.no_alias": ("db", "analytics", ""),
            }
        )
        adapter, _ = make_adapter({})

        schema_relations, members = build_schema_groups(
            adapter, manifest, list(manifest.nodes)
        )

        assert set(schema_relations) == {("db", "analytics"), ("db", "staging")}
        assert members[("db", "analytics")] == {"model.p.a": "a", "model.p.b": "b"}
        assert members[("db", "staging")] == {"model.p.c": "c"}

    def test_node_absent_from_the_manifest_is_skipped(self) -> None:
        manifest = make_manifest({"model.p.a": ("db", "analytics", "a")})
        adapter, _ = make_adapter({})

        _, members = build_schema_groups(
            adapter, manifest, ["model.p.a", "model.p.vanished"]
        )

        assert members[("db", "analytics")] == {"model.p.a": "a"}

    def test_case_differences_group_together(self) -> None:
        manifest = make_manifest(
            {
                "model.p.a": ("DB", "Analytics", "A"),
                "model.p.b": ("db", "analytics", "b"),
            }
        )
        adapter, _ = make_adapter({})

        schema_relations, members = build_schema_groups(
            adapter, manifest, list(manifest.nodes)
        )

        assert list(schema_relations) == [("db", "analytics")]
        assert members[("db", "analytics")] == {"model.p.a": "a", "model.p.b": "b"}


class TestCheckRelationsExist:
    def test_present_and_absent_relations(self) -> None:
        manifest = make_manifest(
            {
                "model.p.here": ("db", "analytics", "here"),
                "model.p.gone": ("db", "analytics", "gone"),
            }
        )
        adapter, _ = make_adapter({("db", "analytics"): ["here", "unrelated"]})

        existence = check_relations_exist(
            adapter, manifest, list(manifest.nodes), parallel=False
        )

        assert existence == {
            "model.p.here": RelationExistence.EXISTS,
            "model.p.gone": RelationExistence.MISSING,
        }

    def test_case_mismatch_still_counts_as_existing(self) -> None:
        """The warehouse may report a different case than dbt configured."""
        manifest = make_manifest({"model.p.a": ("db", "analytics", "my_model")})
        adapter, _ = make_adapter({("db", "analytics"): ["MY_MODEL"]})

        existence = check_relations_exist(
            adapter, manifest, list(manifest.nodes), parallel=False
        )

        assert existence == {"model.p.a": RelationExistence.EXISTS}

    def test_empty_schema_marks_everything_missing(self) -> None:
        manifest = make_manifest(
            {
                "model.p.a": ("db", "analytics", "a"),
                "model.p.b": ("db", "analytics", "b"),
            }
        )
        adapter, _ = make_adapter({("db", "analytics"): []})

        existence = check_relations_exist(
            adapter, manifest, list(manifest.nodes), parallel=False
        )

        assert set(existence.values()) == {RelationExistence.MISSING}

    def test_one_query_per_schema_not_per_node(self) -> None:
        manifest = make_manifest(
            {
                f"model.p.node_{index}": ("db", "analytics", f"node_{index}")
                for index in range(25)
            }
            | {"model.p.other": ("db", "staging", "other")}
        )
        adapter, calls = make_adapter(
            {
                ("db", "analytics"): [f"node_{index}" for index in range(25)],
                ("db", "staging"): ["other"],
            }
        )

        existence = check_relations_exist(
            adapter, manifest, list(manifest.nodes), parallel=False
        )

        assert len(existence) == 26
        assert sorted(calls) == [("db", "analytics"), ("db", "staging")]

    def test_failing_schema_is_unknown_and_isolated(self) -> None:
        manifest = make_manifest(
            {
                "model.p.broken": ("db", "locked", "broken"),
                "model.p.fine": ("db", "analytics", "fine"),
            }
        )
        adapter, _ = make_adapter(
            {
                ("db", "locked"): Exception("permission denied for schema locked"),
                ("db", "analytics"): ["fine"],
            }
        )

        existence = check_relations_exist(
            adapter, manifest, list(manifest.nodes), parallel=False
        )

        assert existence == {
            "model.p.broken": RelationExistence.UNKNOWN,
            "model.p.fine": RelationExistence.EXISTS,
        }

    def test_no_candidates_performs_no_queries(self) -> None:
        adapter, calls = make_adapter({})

        assert (
            check_relations_exist(adapter, make_manifest({}), [], parallel=False) == {}
        )
        assert calls == []

    def test_parallel_path_actually_uses_a_thread_pool(self) -> None:
        """A bare MagicMock config is truthy for `single_threaded`, so config it explicitly
        and confirm a listing actually ran off the test's own thread."""
        pytest.importorskip("dbt_common.utils.executor")
        from dbt_common.context import set_invocation_context

        set_invocation_context({})

        manifest = make_manifest(
            {
                "model.p.a": ("db", "analytics", "a"),
                "model.p.b": ("db", "staging", "b"),
            }
        )
        adapter, calls = make_adapter(
            {("db", "analytics"): ["a"], ("db", "staging"): []}
        )
        adapter.config.args.single_threaded = False
        adapter.config.threads = 4

        main_thread_id = threading.get_ident()
        observed_thread_ids: list[int] = []
        adapter.connection_named.side_effect = lambda name: (
            observed_thread_ids.append(threading.get_ident()) or nullcontext()
        )

        existence = check_relations_exist(
            adapter, manifest, list(manifest.nodes), parallel=True
        )

        assert existence == {
            "model.p.a": RelationExistence.EXISTS,
            "model.p.b": RelationExistence.MISSING,
        }
        assert sorted(calls) == [("db", "analytics"), ("db", "staging")]
        assert any(tid != main_thread_id for tid in observed_thread_ids), (
            "expected at least one schema listing to run on a worker thread; "
            "the threaded path did not actually engage"
        )

    def test_parallel_path_falls_back_to_sequential_when_single_threaded(self) -> None:
        """`--single-threaded` / `threads: 1`: no executor spun up."""
        manifest = make_manifest({"model.p.a": ("db", "analytics", "a")})
        adapter, calls = make_adapter({("db", "analytics"): ["a"]})
        adapter.config.args.single_threaded = True

        existence = check_relations_exist(
            adapter, manifest, list(manifest.nodes), parallel=True
        )

        assert existence == {"model.p.a": RelationExistence.EXISTS}
        assert calls == [("db", "analytics")]


class TestRegistry:
    def test_spark_is_excluded_by_default(self) -> None:
        assert "spark" in EXISTENCE_CHECK_UNSUPPORTED
        assert existence_check_supported("spark") is False
        assert existence_check_supported("snowflake") is True

    def test_no_override_registered_by_default(self) -> None:
        assert EXISTENCE_OVERRIDE_BY_ADAPTER_TYPE == {}
        assert try_registered_override("snowflake", MagicMock(), MagicMock()) is None

    def test_registered_override_replaces_the_dbt_listing(self, monkeypatch) -> None:
        monkeypatch.setitem(
            EXISTENCE_OVERRIDE_BY_ADAPTER_TYPE,
            "postgres",
            lambda adapter, schema_relation: {"A"},
        )
        manifest = make_manifest({"model.p.a": ("db", "analytics", "a")})
        adapter, calls = make_adapter({})

        existence = check_relations_exist(
            adapter, manifest, list(manifest.nodes), parallel=False
        )

        assert existence == {"model.p.a": RelationExistence.EXISTS}
        assert calls == []
        adapter.list_relations_without_caching.assert_not_called()
