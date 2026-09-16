from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

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
        mock_runner.invoke.return_value = Mock(
            success=True, exception=None, result=Mock()
        )
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
        mock_runner.invoke.return_value = Mock(
            success=True, exception=None, result=Mock()
        )
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
        mock_runner.invoke.return_value = Mock(
            success=True, exception=None, result=Mock()
        )
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

    def test_aborts_rather_than_reading_stale_output_when_the_run_failed(self):
        """dbtRunner reports failure via the result rather than raising, and a failed
        run leaves any previous sources.json in place. Reading it would reuse stale
        timestamps and wrongly mark sources unchanged, so bail out and let the caller
        run dbt unmodified."""
        mock_runner = Mock()
        mock_runner.invoke.return_value = Mock(
            success=False, exception=RuntimeError("boom"), result=None
        )
        mock_runner_factory = Mock(return_value=mock_runner)

        stale = {
            "results": [
                {
                    "unique_id": "source.proj.raw.stale",
                    "max_loaded_at": datetime(2020, 1, 1),
                }
            ]
        }

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json", return_value=stale
            ) as load_json:
                result = get_source_freshness(())

        assert result is None
        load_json.assert_not_called()

    def test_aborts_when_the_run_returned_no_result_at_all(self):
        """dbtRunner also returns success=True with result=None (a clean ClickExit).
        There is no fresh output to read in that case either."""
        mock_runner = Mock()
        mock_runner.invoke.return_value = Mock(
            success=True, exception=None, result=None
        )
        mock_runner_factory = Mock(return_value=mock_runner)

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value={"results": []},
            ) as load_json:
                result = get_source_freshness(())

        assert result is None
        load_json.assert_not_called()

    def test_a_stale_source_does_not_abort_the_run(self):
        """A source past its error_after threshold gets FreshnessStatus.Error, which
        shares dbt's 'error' NodeStatus, so interpret_results reports success=False on
        a run that executed perfectly. That is the normal thing source freshness exists
        to detect -- keying the abort off `success` would disable state-aware
        orchestration whenever any source went stale."""
        mock_runner = Mock()
        mock_runner.invoke.return_value = Mock(
            success=False, exception=None, result=Mock()
        )
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.proj.raw.stale_but_real",
                    "max_loaded_at": datetime(2020, 1, 1),
                }
            ]
        }

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value=freshness_result,
            ):
                result = get_source_freshness(())

        assert result == SourceFreshness(
            sources={"source.proj.raw.stale_but_real": datetime(2020, 1, 1)}
        )
