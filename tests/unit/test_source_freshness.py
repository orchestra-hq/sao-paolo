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

    def test_default_ignores_selectors_to_run(self):
        """scope_to_selection defaults to off: selectors_to_run is ignored entirely."""
        assert get_args_for_source_freshness((), selectors_to_run=["proj.a"]) == [
            "source",
            "freshness",
            "-q",
        ]

    def test_scoped_selects_ancestors_of_each_node(self):
        args = get_args_for_source_freshness(
            ("--target", "prod"),
            scope_to_selection=True,
            selectors_to_run=["proj.a", "proj.b"],
        )

        assert args == [
            "source",
            "freshness",
            "-q",
            "--target",
            "prod",
            "--select",
            "+proj.a",
            "+proj.b",
        ]

    def test_scoped_selects_by_fqn_never_by_path(self):
        """The selection must not use `path:`. dbt's PathSelectorMethod globs the
        real filesystem from the project root, so a node owned by an installed
        package -- whose original_file_path is relative to that package -- never
        matches. A project whose models all live in packages would select zero
        nodes and collect zero sources."""
        args = get_args_for_source_freshness(
            (),
            scope_to_selection=True,
            selectors_to_run=["a_package.staging.stg_thing"],
        )

        assert args == [
            "source",
            "freshness",
            "-q",
            "--select",
            "+a_package.staging.stg_thing",
        ]
        assert not any(arg.startswith("+path:") for arg in args)

    def test_scoped_with_no_selectors_to_run_omits_select(self):
        assert get_args_for_source_freshness(
            (), scope_to_selection=True, selectors_to_run=None
        ) == ["source", "freshness", "-q"]

        assert get_args_for_source_freshness(
            (), scope_to_selection=True, selectors_to_run=[]
        ) == ["source", "freshness", "-q"]

    def test_scoped_is_indifferent_to_how_selectors_to_run_was_selected(self):
        """selectors_to_run is already resolved (by dbt ls, elsewhere) by the time
        this runs -- --select, --selector, whatever was used to get there doesn't
        matter, only the resulting nodes do."""
        via_selector = get_args_for_source_freshness(
            ("--selector", "nightly"),
            scope_to_selection=True,
            selectors_to_run=["proj.a"],
        )
        via_select = get_args_for_source_freshness(
            ("--select", "tag:nightly"),
            scope_to_selection=True,
            selectors_to_run=["proj.a"],
        )

        assert (
            via_selector
            == via_select
            == [
                "source",
                "freshness",
                "-q",
                "--select",
                "+proj.a",
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
                    ("--target", "prod"), selectors_to_run=["proj.a"]
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
                    selectors_to_run=["proj.a"],
                )

        mock_runner.invoke.assert_called_once_with(
            args=[
                "source",
                "freshness",
                "-q",
                "--target",
                "prod",
                "--select",
                "+proj.a",
            ]
        )
        assert result == SourceFreshness(
            sources={"source.proj.raw.x": datetime(2026, 3, 31)}
        )

    def test_scoped_still_runs_databricks_fallback_for_a_used_source(self):
        """Scoping restricts which sources dbt selects (--select +<fqn>), it does
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
                    selectors_to_run=["proj.a"],
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


# Tests for view-backed sources with no explicit freshness config, against
# the real dbt-core classes get_source_freshness patches.


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


def _runner_for(adapter: Any):
    """Build the patched runner the way dbt would."""
    from dbt.task.freshness import FreshnessTask

    get_source_freshness(user_args=())
    runner_cls = FreshnessTask.get_runner_type(None, None)  # pyright: ignore[reportArgumentType]
    assert runner_cls is not None
    runner = object.__new__(runner_cls)  # pyright: ignore[reportArgumentType]
    runner.adapter = adapter
    runner.config = None  # pyright: ignore[reportAttributeAccessIssue]
    return runner


def _unconfigured_node(unique_id: str = "source.p.s.x") -> Any:
    return SimpleNamespace(
        freshness=None,
        loaded_at_query=None,
        loaded_at_field=None,
        unique_id=unique_id,
        name=unique_id.rsplit(".", 1)[-1],
        database="db",
        schema="public",
        identifier=unique_id.rsplit(".", 1)[-1],
    )


def test_view_backed_source_never_reaches_metadata_freshness(
    patched_dbt_runner: None,
) -> None:
    """The view check short-circuits before dbt-core's execute() runs."""
    from dbt.artifacts.schemas.results import FreshnessStatus

    class AdapterThatWouldExplodeIfAsked(FakeAdapterNoMetadataSupport):
        def supports(self, capability: Any) -> bool:
            # Only reached if the view short-circuit didn't fire.
            raise AssertionError("should have returned before this")

    runner = _runner_for(AdapterThatWouldExplodeIfAsked())
    result = runner.execute(_unconfigured_node(), manifest={})  # pyright: ignore[reportArgumentType]

    assert result.status == FreshnessStatus.Pass
    assert result.max_loaded_at is not None


def test_table_backed_source_is_not_short_circuited(
    patched_dbt_runner: None,
) -> None:
    """is_view's False must not stop the normal execute() path."""

    class TableAdapter(FakeAdapterNoMetadataSupport):
        def get_relation(self, database: str, schema: str, identifier: str) -> Any:
            return SimpleNamespace(type="table")

    runner = _runner_for(TableAdapter())
    result = runner.execute(_unconfigured_node(), manifest={})  # pyright: ignore[reportArgumentType]

    # Reaches the "unknown" fallback via a real DbtRuntimeError, not
    # the view short-circuit.
    from dbt.artifacts.schemas.results import FreshnessStatus

    assert result.status == FreshnessStatus.Pass


def test_source_with_loaded_at_field_skips_the_view_check(
    patched_dbt_runner: None,
) -> None:
    """Explicit loaded_at_* means the relation kind is never looked up."""

    class CountingAdapter(FakeAdapterNoMetadataSupport):
        def __init__(self) -> None:
            self.get_relation_calls = 0

        def get_relation(self, database: str, schema: str, identifier: str) -> Any:
            self.get_relation_calls += 1
            return SimpleNamespace(type="view")

        def calculate_freshness(self, *args: Any, **kwargs: Any) -> Any:
            now = datetime.now(timezone.utc)
            return None, {"max_loaded_at": now, "snapshotted_at": now, "age": 0}

    adapter = CountingAdapter()
    runner = _runner_for(adapter)

    node = _unconfigured_node("source.p.s.configured")
    node.loaded_at_field = "updated_at"  # opts out of metadata freshness
    runner.execute(node, manifest={})  # pyright: ignore[reportArgumentType]

    assert adapter.get_relation_calls == 0


def test_source_with_adapter_fallback_skips_the_view_check(
    patched_dbt_runner: None,
) -> None:
    """A registered fallback (e.g. Databricks) means the relation kind is
    never looked up."""

    class CountingAdapter(FakeAdapterNoMetadataSupport):
        def __init__(self) -> None:
            self.get_relation_calls = 0

        def type(self) -> str:
            return "databricks"

        def get_relation(self, database: str, schema: str, identifier: str) -> Any:
            self.get_relation_calls += 1
            return SimpleNamespace(type="view")

    adapter = CountingAdapter()
    runner = _runner_for(adapter)

    with patch(
        "src.orchestra_dbt.source_freshness.FALLBACK_BY_ADAPTER_TYPE",
        {"databricks": Mock(return_value=object())},
    ):
        runner.execute(_unconfigured_node(), manifest={})  # pyright: ignore[reportArgumentType]

    assert adapter.get_relation_calls == 0
