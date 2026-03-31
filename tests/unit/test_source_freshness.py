from unittest.mock import Mock, patch

from src.orchestra_dbt.models import SourceFreshness
from src.orchestra_dbt.source_freshness import (
    get_args_for_source_freshness,
    get_source_freshness,
)


class TestGetArgsForSourceFreshness:
    def test_preserves_user_args_in_original_order(self):
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
            "--select",
            "source:raw.orders+",
            "--selector",
            "nightly",
            "--exclude",
            "source:raw.archived_*",
        ]

    def test_filters_command_specific_flags(self):
        user_args = ("--full-refresh", "--empty", "--target", "prod")

        assert get_args_for_source_freshness(user_args) == [
            "source",
            "freshness",
            "-q",
            "--target",
            "prod",
        ]


class TestGetSourceFreshness:
    def test_passes_selector_args_to_dbt_source_freshness(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {"unique_id": "source.project.raw.orders", "max_loaded_at": "2026-03-31"}
            ]
        }

        with patch.dict(
            "sys.modules",
            {
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
            },
        ):
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
                    )
                )

        assert result == SourceFreshness(
            sources={"source.project.raw.orders": "2026-03-31"}
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

    def test_returns_only_sources_selected_by_upstream_model_selection(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.project.raw.selected_upstream_source",
                    "max_loaded_at": "2026-03-31",
                }
            ]
        }

        with patch.dict(
            "sys.modules",
            {
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
            },
        ):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value=freshness_result,
            ):
                result = get_source_freshness(
                    (
                        "--select",
                        "model:stg_selected_orders",
                    )
                )

        assert result == SourceFreshness(
            sources={"source.project.raw.selected_upstream_source": "2026-03-31"}
        )
        assert "source.project.raw.unselected_source" not in result.sources
        mock_runner.invoke.assert_called_once_with(
            args=[
                "source",
                "freshness",
                "-q",
                "--select",
                "model:stg_selected_orders",
            ]
        )
