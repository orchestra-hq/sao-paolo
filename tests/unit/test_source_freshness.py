from datetime import UTC, datetime
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

    def test_failed_run_ignores_the_previous_sources_json(self):
        """dbtRunner reports failure on the result, and a failed run leaves the
        previous sources.json in place."""
        mock_runner = Mock()
        mock_runner.invoke.return_value = Mock(exception=RuntimeError("boom"))
        stale = {
            "results": [
                {
                    "unique_id": "source.proj.raw.x",
                    "max_loaded_at": datetime(2020, 1, 1, tzinfo=UTC),
                }
            ]
        }

        with (
            patch.dict(
                "sys.modules",
                self._patched_dbt_modules(Mock(return_value=mock_runner)),
            ),
            patch(
                "src.orchestra_dbt.source_freshness.load_json", return_value=stale
            ) as load_json,
        ):
            result = get_source_freshness(())

        assert result is None
        load_json.assert_not_called()

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
    """dbt 2.x has no dbt.task.freshness; run dbt's own `dbt source freshness`."""

    def test_runs_dbts_own_freshness(self):
        runner, result = self._run(
            {"results": [_result("source.proj.raw.x")]},
            user_args=("--target", "prod"),
        )

        runner.invoke.assert_called_once_with(
            ["source", "freshness", "-q", "--target", "prod", "--check-all"]
        )
        assert result == SourceFreshness(sources={"source.proj.raw.x": _AT})

    def test_dbt_1_x_without_its_internals_raises_rather_than_falling_back(self):
        with (
            patch("src.orchestra_dbt.source_freshness.is_dbt_v2", return_value=False),
            patch.dict("sys.modules", {"dbt.task.freshness": None}),
            pytest.raises(ImportError),
        ):
            get_source_freshness(())

    def _run(
        self,
        freshness_result,
        manifest=None,
        exceptions=(None,),
        user_args=(),
        **kwargs,
    ):
        """`manifest` defaults to every result's source having a loaded_at_field."""
        if manifest is None:
            manifest = {
                r["unique_id"]: "updated_at" for r in freshness_result["results"]
            }
        runner = Mock()
        runner.invoke.side_effect = [Mock(exception=e) for e in exceptions]
        modules = {"dbt.cli.main": Mock(dbtRunner=Mock(return_value=runner))}

        def fake_load_json(path):
            if path == "target/sources.json":
                return freshness_result
            return {"sources": {uid: _source(uid, f) for uid, f in manifest.items()}}

        with (
            patch("src.orchestra_dbt.source_freshness.is_dbt_v2", return_value=True),
            patch.dict("sys.modules", modules),
            patch(
                "src.orchestra_dbt.source_freshness.load_json",
                side_effect=fake_load_json,
            ),
        ):
            return runner, get_source_freshness(user_args, **kwargs)

    def test_keeps_sources_dbt_resolved_from_metadata(self):
        """dbt 2.x still computes metadata freshness without loaded_at_*; keep those."""
        _, result = self._run(
            {"results": [_result("source.proj.raw.metadata_only")]},
            manifest={"source.proj.raw.metadata_only": None},
        )

        assert result == SourceFreshness(sources={"source.proj.raw.metadata_only": _AT})

    def test_require_explicit_source_freshness_excludes_via_manifest(self):
        """dbt 2.x omits loaded_at_* from sources.json's criteria, hence the manifest."""
        _, result = self._run(
            {
                "results": [
                    _result("source.proj.raw.metadata_only"),
                    _result("source.proj.raw.explicit"),
                ]
            },
            manifest={
                "source.proj.raw.metadata_only": None,
                "source.proj.raw.explicit": "updated_at",
            },
            require_explicit_source_freshness=True,
        )

        assert result == SourceFreshness(sources={"source.proj.raw.explicit": _AT})

    def test_retries_without_implicit_sources_when_the_run_fails(self):
        """duckdb's 2.x engine panics on metadata freshness and aborts every source."""
        runner, result = self._run(
            {"results": [_result("source.proj.raw.explicit")]},
            manifest={
                "source.proj.raw.explicit": "updated_at",
                "source.proj.raw.implicit": None,
            },
            exceptions=(Exception("not yet implemented"), None),
        )

        assert runner.invoke.call_args_list[1].args[0][-2:] == [
            "--exclude",
            "source:proj.raw.implicit",
        ]
        assert result == SourceFreshness(sources={"source.proj.raw.explicit": _AT})

    def test_no_retry_without_implicit_sources(self):
        runner, _ = self._run(
            {"results": [_result("source.proj.raw.explicit")]},
            exceptions=(Exception("boom"),),
        )

        runner.invoke.assert_called_once()

    def test_failed_run_ignores_the_previous_sources_json(self):
        """A failed run writes nothing, so sources.json is the last run's."""
        with patch("src.orchestra_dbt.source_freshness.log_warn") as warn:
            _, result = self._run(
                {"results": [_result("source.proj.raw.x")]},
                exceptions=(Exception("boom"),),
            )

        assert result is None
        assert any("boom" in c.args[0] for c in warn.call_args_list)

    def test_failed_retry_ignores_the_previous_sources_json(self):
        _, result = self._run(
            {"results": [_result("source.proj.raw.explicit")]},
            manifest={
                "source.proj.raw.explicit": "updated_at",
                "source.proj.raw.implicit": None,
            },
            exceptions=(Exception("not yet implemented"), Exception("boom")),
        )

        assert result is None

    def test_keeps_stale_error_status_sources(self):
        """A stale source's max_loaded_at is still a real reading."""
        _, result = self._run(
            {"results": [{**_result("source.proj.raw.stale"), "status": "Error"}]}
        )

        assert result == SourceFreshness(sources={"source.proj.raw.stale": _AT})


_AT = datetime(2026, 3, 31, tzinfo=UTC)


def _result(unique_id: str) -> dict:
    return {"unique_id": unique_id, "max_loaded_at": _AT}


def _source(unique_id: str, loaded_at_field: str | None) -> dict:
    return {
        "fqn": unique_id.split(".")[1:],
        "config": {"loaded_at_field": loaded_at_field, "loaded_at_query": None},
    }
