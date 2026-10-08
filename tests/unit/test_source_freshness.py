from datetime import UTC, datetime
from types import SimpleNamespace
from typing import ClassVar
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

    def test_forwards_profile_resolution_flags(self):
        """Otherwise freshness looks for the profile in ~/.dbt (or under the wrong
        name/vars) and fails, so orc falls back to running without reuse."""
        assert get_args_for_source_freshness(
            (
                "dbt",
                "build",
                "--profiles-dir=proj",
                "--profile",
                "alt",
                "--vars",
                "{schema: x}",
            )
        ) == [
            "source",
            "freshness",
            "-q",
            "--profiles-dir",
            "proj",
            "--profile",
            "alt",
            "--vars",
            "{schema: x}",
        ]

    def test_forwards_empty_flag_value_unchanged(self):
        """Freshness must see what the user's run sees, even an empty value."""
        assert get_args_for_source_freshness(("dbt", "build", "--profiles-dir=")) == [
            "source",
            "freshness",
            "-q",
            "--profiles-dir",
            "",
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
            # Else the real module imports dbt_common.exceptions.events, which the mock
            # below hides, and these tests silently run the 2.x path.
            "dbt.adapters.factory": Mock(FACTORY=Mock(adapters={})),
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
        mock_runner.invoke.return_value = Mock(exception=None)
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.proj.raw.x",
                    "max_loaded_at": datetime(2026, 3, 31, tzinfo=UTC),
                },
                {
                    "unique_id": "source.proj.raw.y",
                    "max_loaded_at": datetime(2026, 3, 30, tzinfo=UTC),
                },
            ]
        }

        with (
            patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)),
            patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value=freshness_result,
            ),
        ):
            result = get_source_freshness(
                ("--target", "prod"), selectors_to_run=["proj.a"]
            )

        mock_runner.invoke.assert_called_once_with(
            args=["source", "freshness", "-q", "--target", "prod"]
        )
        assert result == SourceFreshness(
            sources={
                "source.proj.raw.x": datetime(2026, 3, 31, tzinfo=UTC),
                "source.proj.raw.y": datetime(2026, 3, 30, tzinfo=UTC),
            }
        )

    def test_scoped_passes_ancestor_selection_to_dbt(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = Mock(exception=None)
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.proj.raw.x",
                    "max_loaded_at": datetime(2026, 3, 31, tzinfo=UTC),
                }
            ]
        }

        with (
            patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)),
            patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value=freshness_result,
            ),
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
            sources={"source.proj.raw.x": datetime(2026, 3, 31, tzinfo=UTC)}
        )

    def test_scoped_still_runs_databricks_fallback_for_a_used_source(self):
        """Scoping restricts which sources dbt selects (--select +<fqn>), it does
        not change what the runner does for a source dbt does select. A Databricks
        source with no loaded_at_field/query -- in scope -- must still hit the
        DESCRIBE HISTORY fallback, exactly as when scoping is off."""
        mock_runner = Mock()
        mock_runner.invoke.return_value = Mock(exception=None)
        mock_runner_factory = Mock(return_value=mock_runner)
        modules = self._patched_dbt_modules(mock_runner_factory)

        with (
            patch.dict("sys.modules", modules),
            patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value={"results": []},
            ),
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


class TestGetSourceFreshnessOnDbtCoreV2:
    """dbt 2.x has no dbt.task.freshness; fall back to `dbt source freshness`."""

    _V1_ONLY_MODULES: ClassVar[dict[str, None]] = {
        "dbt.artifacts.resources.v1.components": None,
        "dbt.artifacts.schemas.freshness": None,
        "dbt.artifacts.schemas.freshness.v3.freshness": None,
        "dbt.artifacts.schemas.results": None,
        "dbt.task.freshness": None,
        "dbt_common.exceptions": None,
    }

    def test_falls_back_to_native_freshness_when_v1_internals_are_unavailable(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = Mock(exception=None)
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.proj.raw.x",
                    "max_loaded_at": datetime(2026, 3, 31, tzinfo=UTC),
                    "criteria": {"loaded_at_field": "updated_at"},
                },
            ]
        }

        modules = {
            **self._V1_ONLY_MODULES,
            "dbt.cli.main": Mock(dbtRunner=mock_runner_factory),
        }

        with (
            patch.dict("sys.modules", modules),
            patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value=freshness_result,
            ),
        ):
            result = get_source_freshness(("--target", "prod"))

        mock_runner.invoke.assert_called_once_with(
            args=["source", "freshness", "-q", "--target", "prod"]
        )
        assert result == SourceFreshness(
            sources={"source.proj.raw.x": datetime(2026, 3, 31, tzinfo=UTC)}
        )

    def test_still_raises_when_dbt_core_is_entirely_missing(self):
        modules = {**self._V1_ONLY_MODULES, "dbt.cli.main": None}

        with patch.dict("sys.modules", modules), pytest.raises(ImportError):
            get_source_freshness(())

    def _run(self, freshness_result, manifest=None, exception=None, **kwargs):
        mock_runner = Mock()
        mock_runner.invoke.return_value = Mock(exception=exception)
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

        with (
            patch.dict("sys.modules", modules),
            patch(
                "src.orchestra_dbt.source_freshness.load_json",
                side_effect=fake_load_json,
            ),
        ):
            return get_source_freshness((), **kwargs)

    def test_keeps_sources_dbt_resolved_from_metadata(self):
        """dbt 2.x still computes metadata freshness without loaded_at_*; keep those."""
        result = self._run(
            {
                "results": [
                    {
                        "unique_id": "source.proj.raw.metadata_only",
                        "max_loaded_at": datetime(2026, 3, 31, tzinfo=UTC),
                        "criteria": {"loaded_at_field": None, "loaded_at_query": None},
                    }
                ]
            }
        )

        assert result == SourceFreshness(
            sources={"source.proj.raw.metadata_only": datetime(2026, 3, 31, tzinfo=UTC)}
        )

    _TWO_SOURCES: ClassVar[dict] = {
        "results": [
            # dbt 2.x omits loaded_at_* from criteria, hence the manifest lookup.
            {
                "unique_id": "source.proj.raw.metadata_only",
                "max_loaded_at": datetime(2026, 3, 31, tzinfo=UTC),
                "criteria": {"error_after": {"count": 24, "period": "hour"}},
            },
            {
                "unique_id": "source.proj.raw.explicit",
                "max_loaded_at": datetime(2026, 3, 30, tzinfo=UTC),
                "criteria": {"error_after": {"count": 24, "period": "hour"}},
            },
        ]
    }

    def test_require_explicit_source_freshness_excludes_via_manifest(self):
        result = self._run(
            self._TWO_SOURCES,
            manifest={
                "sources": {
                    "source.proj.raw.metadata_only": {
                        "config": {"loaded_at_field": None, "loaded_at_query": None}
                    },
                    "source.proj.raw.explicit": {
                        "config": {
                            "loaded_at_field": "updated_at",
                            "loaded_at_query": None,
                        }
                    },
                }
            },
            require_explicit_source_freshness=True,
        )

        assert result == SourceFreshness(
            sources={"source.proj.raw.explicit": datetime(2026, 3, 30, tzinfo=UTC)}
        )

    def test_sources_dbt_never_checked_are_not_treated_as_failures(self):
        """Sources without a `freshness:` block never reach sources.json."""
        result = self._run(
            {
                "results": [
                    {
                        "unique_id": "source.proj.raw.checked",
                        "max_loaded_at": datetime(2026, 3, 31, tzinfo=UTC),
                        "criteria": {"error_after": {"count": 24, "period": "hour"}},
                    }
                ]
            },
            manifest={
                "sources": {
                    "source.proj.raw.checked": {
                        "config": {
                            "loaded_at_field": "updated_at",
                            "loaded_at_query": None,
                        }
                    },
                    "source.proj.raw.never_checked": {
                        "config": {
                            "loaded_at_field": "updated_at",
                            "loaded_at_query": None,
                        }
                    },
                    "source.proj.raw.no_freshness_block": {
                        "config": {"loaded_at_field": None, "loaded_at_query": None}
                    },
                }
            },
            require_explicit_source_freshness=True,
        )

        assert result == SourceFreshness(
            sources={"source.proj.raw.checked": datetime(2026, 3, 31, tzinfo=UTC)}
        )

    def test_engine_error_warns(self):
        with patch("src.orchestra_dbt.source_freshness.log_warn") as warn:
            self._run({"results": []}, exception=Exception("boom"))
        assert any("boom" in c.args[0] for c in warn.call_args_list)

    def test_keeps_stale_error_status_sources(self):
        """A stale source's max_loaded_at is still a real reading."""
        result = self._run(
            {
                "results": [
                    {
                        "unique_id": "source.proj.raw.stale",
                        "max_loaded_at": datetime(2026, 5, 26, tzinfo=UTC),
                        "status": "Error",
                        "criteria": {"error_after": {"count": 24, "period": "hour"}},
                    }
                ]
            }
        )

        assert result == SourceFreshness(
            sources={"source.proj.raw.stale": datetime(2026, 5, 26, tzinfo=UTC)}
        )
