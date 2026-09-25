from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

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


class TestGetSourceFreshnessOnDbtCoreV2:
    """dbt-core >=2.0 (Fusion) drops dbt.task.freshness / dbt.adapters, so
    `get_source_freshness` must fall back to running dbt's native, unpatched
    `dbt source freshness` instead of crashing."""

    _V1_ONLY_MODULES = {
        "dbt.artifacts.resources.v1.components": None,
        "dbt.artifacts.schemas.freshness": None,
        "dbt.artifacts.schemas.freshness.v3.freshness": None,
        "dbt.artifacts.schemas.results": None,
        "dbt.task.freshness": None,
        "dbt_common.exceptions": None,
    }

    def test_falls_back_to_native_freshness_when_v1_internals_are_unavailable(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.proj.raw.x",
                    "max_loaded_at": datetime(2026, 3, 31),
                    "criteria": {"loaded_at_field": "updated_at"},
                },
                {
                    "unique_id": "source.proj.raw.no_config",
                    "max_loaded_at": None,
                    "criteria": {"loaded_at_field": None, "loaded_at_query": None},
                },
            ]
        }

        modules = {
            **self._V1_ONLY_MODULES,
            "dbt.cli.main": Mock(dbtRunner=mock_runner_factory),
        }

        with patch.dict("sys.modules", modules):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value=freshness_result,
            ):
                result = get_source_freshness(("--target", "prod"))

        mock_runner.invoke.assert_called_once_with(
            args=["source", "freshness", "-q", "--target", "prod"]
        )
        assert result == SourceFreshness(
            sources={"source.proj.raw.x": datetime(2026, 3, 31)}
        )

    def test_still_raises_when_dbt_core_is_entirely_missing(self):
        modules = {**self._V1_ONLY_MODULES, "dbt.cli.main": None}

        with patch.dict("sys.modules", modules):
            with pytest.raises(ImportError):
                get_source_freshness(())

    def _run(self, freshness_result, manifest=None, **kwargs):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        modules = {
            **self._V1_ONLY_MODULES,
            "dbt.cli.main": Mock(dbtRunner=Mock(return_value=mock_runner)),
        }

        def fake_load_json(path):
            if path == "target/sources.json":
                return freshness_result
            if manifest is None:
                raise FileNotFoundError(path)
            return manifest

        with patch.dict("sys.modules", modules):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                side_effect=fake_load_json,
            ):
                return get_source_freshness((), **kwargs)

    def test_keeps_sources_dbt_resolved_from_metadata(self):
        """dbt 2.x still computes freshness from warehouse metadata when a source sets
        neither loaded_at_field nor loaded_at_query. Dropping those would leave state-aware
        orchestration inert on any project that does not configure loaded_at_* everywhere."""
        result = self._run(
            {
                "results": [
                    {
                        "unique_id": "source.proj.raw.metadata_only",
                        "max_loaded_at": datetime(2026, 3, 31),
                        "criteria": {"loaded_at_field": None, "loaded_at_query": None},
                    }
                ]
            }
        )

        assert result == SourceFreshness(
            sources={"source.proj.raw.metadata_only": datetime(2026, 3, 31)}
        )

    _TWO_SOURCES = {
        "results": [
            # dbt 2.x omits loaded_at_field/loaded_at_query from criteria entirely,
            # even for a source that sets one -- hence the manifest lookup.
            {
                "unique_id": "source.proj.raw.metadata_only",
                "max_loaded_at": datetime(2026, 3, 31),
                "criteria": {"error_after": {"count": 24, "period": "hour"}},
            },
            {
                "unique_id": "source.proj.raw.explicit",
                "max_loaded_at": datetime(2026, 3, 30),
                "criteria": {"error_after": {"count": 24, "period": "hour"}},
            },
        ]
    }

    def test_require_explicit_source_freshness_excludes_via_manifest(self):
        result = self._run(
            self._TWO_SOURCES,
            manifest={
                "sources": {
                    "source.proj.raw.metadata_only": {"loaded_at_field": None},
                    "source.proj.raw.explicit": {"loaded_at_field": "updated_at"},
                }
            },
            require_explicit_source_freshness=True,
        )

        assert result == SourceFreshness(
            sources={"source.proj.raw.explicit": datetime(2026, 3, 30)}
        )

    def test_require_explicit_reads_loaded_at_field_from_config(self):
        """dbt 2.x duplicates both onto the node's `config`; a source that only carries
        it there is still explicit."""
        result = self._run(
            self._TWO_SOURCES,
            manifest={
                "sources": {
                    "source.proj.raw.metadata_only": {
                        "freshness": {"error_after": {"count": 24, "period": "hour"}}
                    },
                    "source.proj.raw.explicit": {
                        "config": {"loaded_at_field": "_fivetran_synced"}
                    },
                }
            },
            require_explicit_source_freshness=True,
        )

        assert result == SourceFreshness(
            sources={"source.proj.raw.explicit": datetime(2026, 3, 30)}
        )

    def test_sources_dbt_never_checked_are_not_treated_as_failures(self):
        """Only sources with a `freshness:` block reach sources.json. The ones without
        one are absent, not failed, and must not be counted or excluded."""
        result = self._run(
            {
                "results": [
                    {
                        "unique_id": "source.proj.raw.checked",
                        "max_loaded_at": datetime(2026, 3, 31),
                        "criteria": {"error_after": {"count": 24, "period": "hour"}},
                    }
                ]
            },
            manifest={
                "sources": {
                    "source.proj.raw.checked": {"loaded_at_field": "updated_at"},
                    "source.proj.raw.never_checked": {"loaded_at_field": "updated_at"},
                    "source.proj.raw.no_freshness_block": {},
                }
            },
            require_explicit_source_freshness=True,
        )

        assert result == SourceFreshness(
            sources={"source.proj.raw.checked": datetime(2026, 3, 31)}
        )

    def test_require_explicit_keeps_everything_when_manifest_unreadable(self):
        """Excluding every source because the manifest is missing would silently turn
        state-aware orchestration off. Warn and keep instead."""
        result = self._run(self._TWO_SOURCES, require_explicit_source_freshness=True)

        assert result == SourceFreshness(
            sources={
                "source.proj.raw.metadata_only": datetime(2026, 3, 31),
                "source.proj.raw.explicit": datetime(2026, 3, 30),
            }
        )

    def test_stale_sources_do_not_warn(self):
        """dbt 2.x reports a stale source as success=False with no exception. Warning on
        that would cry wolf every run on a project with a permanently stale source."""
        from src.orchestra_dbt.source_freshness import _log_freshness_outcome

        handled = SimpleNamespace(success=False, exception=None, exit_code=1)
        with patch("src.orchestra_dbt.source_freshness.log_warn") as warn:
            _log_freshness_outcome(handled, [{"status": "Error"}, {"status": "Pass"}])
        warn.assert_not_called()

    def test_real_engine_error_still_warns(self):
        from src.orchestra_dbt.source_freshness import _log_freshness_outcome

        errored = SimpleNamespace(
            success=False, exception=Exception("boom"), exit_code=2
        )
        with patch("src.orchestra_dbt.source_freshness.log_warn") as warn:
            _log_freshness_outcome(errored, [])
        assert "boom" in warn.call_args[0][0]

    def test_keeps_stale_error_status_sources(self):
        """status Error means the data is stale, not that the reading is bad -- its
        max_loaded_at is exactly the signal reuse needs."""
        result = self._run(
            {
                "results": [
                    {
                        "unique_id": "source.proj.raw.stale",
                        "max_loaded_at": datetime(2026, 5, 26),
                        "status": "Error",
                        "criteria": {"error_after": {"count": 24, "period": "hour"}},
                    }
                ]
            }
        )

        assert result == SourceFreshness(
            sources={"source.proj.raw.stale": datetime(2026, 5, 26)}
        )

    def test_drops_sources_with_no_max_loaded_at(self):
        result = self._run(
            {
                "results": [
                    {
                        "unique_id": "source.proj.raw.unresolved",
                        "max_loaded_at": None,
                        "criteria": {"loaded_at_field": "updated_at"},
                    }
                ]
            }
        )

        assert result == SourceFreshness(sources={})
