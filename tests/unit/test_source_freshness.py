from datetime import datetime
from unittest.mock import Mock, patch

from src.orchestra_dbt.models import SourceFreshness
from src.orchestra_dbt.source_freshness import (
    get_args_for_source_freshness,
    get_source_freshness,
)


class TestGetArgsForSourceFreshness:
    def test_default_only_forwards_target(self):
        """`scope_to_selection` defaults to off, matching dbt's own freshness
        behaviour of checking every source regardless of what's being built."""
        user_args = (
            "--target",
            "prod",
            "--select",
            "source:raw.orders+",
            "--selector",
            "nightly",
            "--exclude",
            "source:raw.archived_*",
        )

        assert get_args_for_source_freshness(user_args) == [
            "source",
            "freshness",
            "-q",
            "--target",
            "prod",
        ]

    def test_default_with_no_target_forwards_nothing(self):
        user_args = ("--select", "source:raw.orders+")

        assert get_args_for_source_freshness(user_args) == [
            "source",
            "freshness",
            "-q",
        ]

    def test_scoped_preserves_user_args_in_original_order(self):
        user_args = (
            "--target",
            "prod",
            "--select",
            "source:raw.orders+",
            "--selector",
            "nightly",
            "--exclude",
            "source:raw.archived_*",
        )

        assert get_args_for_source_freshness(user_args, scope_to_selection=True) == [
            "source",
            "freshness",
            "-q",
            "--target",
            "prod",
            "--select",
            "source:raw.orders+",
            "--selector",
            "nightly",
            "--exclude",
            "source:raw.archived_*",
        ]

    def test_scoped_filters_command_specific_flags(self):
        user_args = ("--full-refresh", "--empty", "--target", "prod")

        assert get_args_for_source_freshness(user_args, scope_to_selection=True) == [
            "source",
            "freshness",
            "-q",
            "--target",
            "prod",
        ]


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

    def test_default_ignores_selector_args_and_checks_every_source(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.project.raw.orders",
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
                    (
                        "--target",
                        "prod",
                        "--select",
                        "source:raw.orders+",
                    )
                )

        assert result == SourceFreshness(
            sources={"source.project.raw.orders": datetime(2026, 3, 31)}
        )
        mock_runner.invoke.assert_called_once_with(
            args=["source", "freshness", "-q", "--target", "prod"]
        )

    def test_scoped_passes_selector_args_to_dbt_source_freshness(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.project.raw.orders",
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
                    (
                        "--target",
                        "prod",
                        "--select",
                        "source:raw.orders+",
                        "--selector",
                        "nightly",
                        "--exclude",
                        "source:raw.archived_*",
                    ),
                    scope_to_selection=True,
                )

        assert result == SourceFreshness(
            sources={"source.project.raw.orders": datetime(2026, 3, 31)}
        )
        mock_runner.invoke.assert_called_once_with(
            args=[
                "source",
                "freshness",
                "-q",
                "--target",
                "prod",
                "--select",
                "source:raw.orders+",
                "--selector",
                "nightly",
                "--exclude",
                "source:raw.archived_*",
            ]
        )

    def test_scoped_returns_only_sources_selected_by_upstream_model_selection(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        selected_source = "source.project.raw.selected_upstream_source"
        unselected_source = "source.project.raw.other_source"
        freshness_result = {
            "results": [
                {
                    "unique_id": selected_source,
                    "max_loaded_at": datetime(2026, 3, 31),
                },
                {
                    "unique_id": unselected_source,
                    "max_loaded_at": datetime(2026, 3, 30),
                },
            ]
        }

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value={"results": [freshness_result["results"][0]]},
            ):
                result = get_source_freshness(
                    (
                        "--select",
                        "model:stg_selected_orders",
                    ),
                    scope_to_selection=True,
                )

        assert result == SourceFreshness(
            sources={selected_source: datetime(2026, 3, 31)}
        )
        assert result is not None
        assert unselected_source not in result.sources
        mock_runner.invoke.assert_called_once_with(
            args=[
                "source",
                "freshness",
                "-q",
                "--select",
                "model:stg_selected_orders",
            ]
        )
