from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest

import src.orchestra_dbt.source_freshness as source_freshness_module
from src.orchestra_dbt.models import SourceFreshness
from src.orchestra_dbt.source_freshness import (
    get_args_for_source_freshness,
    get_source_freshness,
)


class TestGetArgsForSourceFreshness:
    def test_forwards_target_when_present(self):
        user_args = ("--target", "prod", "--select", "my_model")

        assert get_args_for_source_freshness(user_args) == [
            "source",
            "freshness",
            "-q",
            "--target",
            "prod",
        ]

    def test_omits_target_when_absent(self):
        assert get_args_for_source_freshness(("--select", "my_model")) == [
            "source",
            "freshness",
            "-q",
        ]

    def test_default_ignores_paths_to_run(self):
        """scope_to_selection defaults to off: paths_to_run is ignored entirely."""
        assert get_args_for_source_freshness((), paths_to_run=["models/a.sql"]) == [
            "source",
            "freshness",
            "-q",
        ]

    def test_scoped_selects_ancestors_of_each_path(self):
        args = get_args_for_source_freshness(
            ("--target", "prod"),
            scope_to_selection=True,
            paths_to_run=["models/a.sql", "models/b.sql"],
        )

        assert args == [
            "source",
            "freshness",
            "-q",
            "--target",
            "prod",
            "--select",
            "+path:models/a.sql",
            "+path:models/b.sql",
        ]

    def test_scoped_with_no_paths_to_run_omits_select(self):
        assert get_args_for_source_freshness(
            (), scope_to_selection=True, paths_to_run=None
        ) == ["source", "freshness", "-q"]

        assert get_args_for_source_freshness(
            (), scope_to_selection=True, paths_to_run=[]
        ) == ["source", "freshness", "-q"]

    def test_scoped_is_indifferent_to_how_paths_to_run_was_selected(self):
        """paths_to_run is already resolved (by dbt ls, elsewhere) by the time this
        runs -- --select, --selector, whatever was used to get there doesn't matter,
        only the resulting paths do."""
        via_selector = get_args_for_source_freshness(
            ("--selector", "nightly"),
            scope_to_selection=True,
            paths_to_run=["models/a.sql"],
        )
        via_select = get_args_for_source_freshness(
            ("--select", "tag:nightly"),
            scope_to_selection=True,
            paths_to_run=["models/a.sql"],
        )

        assert (
            via_selector
            == via_select
            == [
                "source",
                "freshness",
                "-q",
                "--select",
                "+path:models/a.sql",
            ]
        )


class TestGetSourceFreshness:
    def _patched_dbt_modules(self, mock_runner_factory):
        return {
            "dbt.artifacts.resources.v1.components": Mock(FreshnessThreshold=object),
            "dbt.artifacts.schemas.freshness": Mock(
                SourceDefinition=type("SourceDefinition", (), {"has_freshness": False})
            ),
            "dbt.artifacts.schemas.freshness.v3.freshness": Mock(
                SourceFreshnessResult=object
            ),
            "dbt.artifacts.schemas.results": Mock(
                FreshnessStatus=type("FreshnessStatus", (), {"Pass": "pass"})
            ),
            "dbt.cli.main": Mock(dbtRunner=mock_runner_factory),
            "dbt.task.freshness": Mock(
                FreshnessRunner=type("FreshnessRunner", (), {}),
                FreshnessTask=type("FreshnessTask", (), {}),
            ),
            "dbt_common.exceptions": Mock(DbtRuntimeError=Exception),
        }

    def test_default_checks_every_source_and_ignores_selection(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.proj.raw.x",
                    "max_loaded_at": datetime(2026, 3, 31),
                },
                {
                    "unique_id": "source.proj.raw.y",
                    "max_loaded_at": datetime(2026, 3, 30),
                },
            ]
        }

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value=freshness_result,
            ):
                result = get_source_freshness(
                    ("--target", "prod"), paths_to_run=["models/a.sql"]
                )

        mock_runner.invoke.assert_called_once_with(
            args=["source", "freshness", "-q", "--target", "prod"]
        )
        assert result == SourceFreshness(
            sources={
                "source.proj.raw.x": datetime(2026, 3, 31),
                "source.proj.raw.y": datetime(2026, 3, 30),
            }
        )

    def test_scoped_passes_ancestor_selection_to_dbt(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.proj.raw.x",
                    "max_loaded_at": datetime(2026, 3, 31),
                }
            ]
        }

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value=freshness_result,
            ):
                result = get_source_freshness(
                    ("--target", "prod"),
                    scope_to_selection=True,
                    paths_to_run=["models/a.sql"],
                )

        mock_runner.invoke.assert_called_once_with(
            args=[
                "source",
                "freshness",
                "-q",
                "--target",
                "prod",
                "--select",
                "+path:models/a.sql",
            ]
        )
        assert result == SourceFreshness(
            sources={"source.proj.raw.x": datetime(2026, 3, 31)}
        )

    def test_scoped_still_runs_databricks_fallback_for_a_used_source(self):
        """Scoping restricts which sources dbt selects (--select +path:X), it does
        not change what the runner does for a source dbt does select. A Databricks
        source with no loaded_at_field/query -- in scope -- must still hit the
        DESCRIBE HISTORY fallback, exactly as when scoping is off."""
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)
        modules = self._patched_dbt_modules(mock_runner_factory)

        with patch.dict("sys.modules", modules):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value={"results": []},
            ):
                get_source_freshness(
                    (),
                    scope_to_selection=True,
                    paths_to_run=["models/a.sql"],
                )

        freshness_task = modules["dbt.task.freshness"].FreshnessTask
        orchestra_freshness_runner = freshness_task.get_runner_type(None, None)
        runner = orchestra_freshness_runner()
        runner.adapter = SimpleNamespace(type=lambda: "databricks")

        fallback_result = object()
        fake_handler = Mock(return_value=fallback_result)
        compiled_node = SimpleNamespace(
            freshness=object(),
            loaded_at_field=None,
            loaded_at_query=None,
            unique_id="source.proj.raw.used_by_selection",
        )

        with patch(
            "src.orchestra_dbt.source_freshness.FALLBACK_BY_ADAPTER_TYPE",
            {"databricks": fake_handler},
        ):
            result = runner.execute(compiled_node, manifest=None)

        fake_handler.assert_called_once_with(runner, compiled_node, None)
        assert result is fallback_result


# ---------------------------------------------------------------------------
# Tests for get_source_freshness's handling of sources with no freshness
# config, against the real dbt-core classes it patches.
#
# Vanilla `dbt source freshness` skips any source table that has no
# `freshness:` block configured. get_source_freshness overrides that: it
# forces `SourceDefinition.has_freshness = True` so dbt never skips such
# sources, then patches the freshness runner so an unconfigured source still
# produces a `Pass` result instead of erroring or being omitted -- and, for
# sources relying on implicit relation-metadata freshness, records the
# relation's warehouse kind so a view's frozen timestamp isn't mistaken for
# "no new data".
# ---------------------------------------------------------------------------

class FakeDbtRunner:
    """Stands in for dbt.cli.main.dbtRunner so no real dbt invocation happens."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def invoke(self, args: list[str] | None = None) -> None:
        return None


class FakeRelation:
    @staticmethod
    def create_from(config: Any, node: Any) -> Any:
        return SimpleNamespace(
            database=getattr(node, "database", "db"),
            schema=getattr(node, "schema", "public"),
            identifier=getattr(node, "identifier", "tbl"),
        )


class FakeAdapterNoMetadataSupport:
    """An adapter with no loaded_at_field/query and no metadata-freshness
    support — the real dbt-core FreshnessRunner.execute() has no way to
    compute freshness in this case and raises DbtRuntimeError, which
    OrchestraFreshnessRunner is expected to catch."""

    Relation = FakeRelation

    def type(self) -> str:
        return "unknown_adapter"

    @contextmanager
    def connection_named(self, name: str, node: Any):
        yield

    def clear_transaction(self) -> None:
        pass

    def supports(self, capability: Any) -> bool:
        return False

    def get_relation(self, database: str, schema: str, identifier: str) -> Any:
        return SimpleNamespace(type="view")


@pytest.fixture
def patched_dbt_runner(monkeypatch: pytest.MonkeyPatch):
    """Stop a real dbt invocation, and undo the module's class-level patches.

    get_source_freshness monkeypatches dbt-core classes in place; restoring
    them afterwards keeps tests isolated from each other.
    """
    pytest.importorskip("dbt.artifacts")
    from dbt.artifacts.schemas.freshness import SourceDefinition
    from dbt.task.freshness import FreshnessTask

    monkeypatch.setattr("dbt.cli.main.dbtRunner", FakeDbtRunner)

    def raise_not_found(_path: str) -> dict:
        raise FileNotFoundError("no dbt run happened in this test")

    monkeypatch.setattr(source_freshness_module, "load_json", raise_not_found)

    patched = [
        (SourceDefinition, "has_freshness"),
        (FreshnessTask, "get_runner_type"),
    ]
    originals = [(cls, name, cls.__dict__.get(name)) for cls, name in patched]
    yield
    for cls, name, original in originals:
        if original is not None:
            setattr(cls, name, original)


def test_get_source_freshness_forces_has_freshness_true(
    patched_dbt_runner: None,
) -> None:
    """Sources without a freshness: block must never be skipped."""
    from dbt.artifacts.schemas.freshness import SourceDefinition

    get_source_freshness(user_args=())
    # dbt-core skips freshness checks for sources where has_freshness is
    # False. Forcing it True is what makes "no freshness config" mean
    # "always run", not "always skipped".
    assert SourceDefinition.has_freshness is True


def test_unconfigured_source_runs_and_passes_instead_of_being_skipped(
    patched_dbt_runner: None,
) -> None:
    """A source with no freshness config and no fallback still returns Pass."""
    from dbt.artifacts.resources.v1.components import FreshnessThreshold
    from dbt.artifacts.schemas.results import FreshnessStatus
    from dbt.task.freshness import FreshnessTask

    get_source_freshness(user_args=())
    runner_cls = FreshnessTask.get_runner_type(None, None)  # pyright: ignore[reportArgumentType]
    assert runner_cls is not None

    # Bypass FreshnessRunner.__init__ (needs a real RuntimeConfig); the
    # execute() path under test only touches self.adapter and self.config.
    runner = object.__new__(runner_cls)  # pyright: ignore[reportArgumentType]
    runner.adapter = FakeAdapterNoMetadataSupport()  # pyright: ignore[reportAttributeAccessIssue]
    runner.config = None  # pyright: ignore[reportAttributeAccessIssue]

    compiled_node = SimpleNamespace(
        freshness=None,  # no freshness: block defined on the source
        loaded_at_query=None,
        loaded_at_field=None,
        unique_id="source.test_project.test_schema.test_table",
        name="test_table",
    )

    result = runner.execute(compiled_node, manifest={})  # pyright: ignore[reportArgumentType]

    # A None freshness config is swapped for an empty threshold rather
    # than left as None (which would blow up dbt-core's execute path).
    assert isinstance(compiled_node.freshness, FreshnessThreshold)
    # No adapter fallback is registered for "unknown_adapter", and the
    # real dbt-core execute() can't compute freshness without a
    # loaded_at_field/query or metadata support, so it raises. The
    # runner catches that and synthesizes a Pass result instead of
    # skipping or erroring the source.
    assert result.status == FreshnessStatus.Pass
    assert result.node is compiled_node
    assert result.max_loaded_at is not None
    assert result.snapshotted_at is not None


def _runner_for(state, adapter):
    """Build the patched runner the way dbt would, for a given state."""
    from dbt.task.freshness import FreshnessTask

    get_source_freshness(user_args=(), state=state)
    runner_cls = FreshnessTask.get_runner_type(None, None)  # pyright: ignore[reportArgumentType]
    assert runner_cls is not None
    runner = object.__new__(runner_cls)  # pyright: ignore[reportArgumentType]
    runner.adapter = adapter
    runner.config = None  # pyright: ignore[reportAttributeAccessIssue]
    return runner


def test_relation_type_is_written_back_onto_state_in_place(
    patched_dbt_runner: None,
) -> None:
    """The relation-type cache lives on state, not on the returned value.

    For a source with no loaded_at_* and no adapter fallback, freshness comes
    from relation metadata -- so execute() records the relation kind onto the
    caller's own state while it still holds a connection.
    """
    from src.orchestra_dbt.models import StateApiModel

    state = StateApiModel(state={}, source_relation_types={"db.public.cached": "table"})
    runner = _runner_for(state, FakeAdapterNoMetadataSupport())

    node = SimpleNamespace(
        freshness=None,
        loaded_at_query=None,
        loaded_at_field=None,
        unique_id="source.p.s.new_view",
        name="new_view",
        database="db",
        schema="public",
        identifier="new_view",
        relation_name="db.public.new_view",
    )
    runner.execute(node, manifest={})  # pyright: ignore[reportArgumentType]

    # Newly discovered kind landed on the caller's state as a plain string,
    # and the previously cached entry survived.
    assert state.source_relation_types == {
        "db.public.cached": "table",
        "db.public.new_view": "view",
    }


def test_cached_source_is_not_looked_up_again(patched_dbt_runner: None) -> None:
    """A source already in state costs no relation lookup."""
    from src.orchestra_dbt.models import StateApiModel

    class CountingAdapter(FakeAdapterNoMetadataSupport):
        def __init__(self) -> None:
            self.get_relation_calls = 0

        def get_relation(self, database: str, schema: str, identifier: str) -> Any:
            self.get_relation_calls += 1
            return SimpleNamespace(type="view")

    cached_uid = "source.p.s.known"
    state = StateApiModel(state={}, source_relation_types={"db.public.known": "view"})
    adapter = CountingAdapter()
    runner = _runner_for(state, adapter)

    node = SimpleNamespace(
        freshness=None,
        loaded_at_query=None,
        loaded_at_field=None,
        unique_id=cached_uid,
        name="known",
        database="db",
        schema="public",
        identifier="known",
        relation_name="db.public.known",
    )
    runner.execute(node, manifest={})  # pyright: ignore[reportArgumentType]

    assert adapter.get_relation_calls == 0
    assert state.source_relation_types == {"db.public.known": "view"}


def test_source_with_loaded_at_field_is_not_looked_up(
    patched_dbt_runner: None,
) -> None:
    """Explicit loaded_at_* reads real data, so the relation kind is moot."""
    from src.orchestra_dbt.models import StateApiModel

    class CountingAdapter(FakeAdapterNoMetadataSupport):
        def __init__(self) -> None:
            self.get_relation_calls = 0

        def get_relation(self, database: str, schema: str, identifier: str) -> Any:
            self.get_relation_calls += 1
            return SimpleNamespace(type="view")

        def calculate_freshness(self, *args: Any, **kwargs: Any) -> Any:
            now = datetime.now(timezone.utc)
            return None, {"max_loaded_at": now, "snapshotted_at": now, "age": 0}

    state = StateApiModel(state={})
    adapter = CountingAdapter()
    runner = _runner_for(state, adapter)

    node = SimpleNamespace(
        freshness=None,
        loaded_at_query=None,
        loaded_at_field="updated_at",  # opts out of metadata freshness
        unique_id="source.p.s.configured",
        name="configured",
        database="db",
        schema="public",
        identifier="configured",
        relation_name="db.public.configured",
    )
    runner.execute(node, manifest={})  # pyright: ignore[reportArgumentType]

    assert adapter.get_relation_calls == 0
    assert state.source_relation_types == {}
