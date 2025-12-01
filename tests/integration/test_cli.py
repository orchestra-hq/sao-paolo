from unittest.mock import Mock, patch

from click.testing import CliRunner

from orchestra_dbt.cli import dbt
from orchestra_dbt.models import Freshness, Node, NodeType, ParsedDag, StateApiModel


class TestDbtCommand:
    @patch("orchestra_dbt.cli.validate_environment")
    @patch("orchestra_dbt.cli.get_source_freshness")
    @patch("orchestra_dbt.cli.load_state")
    @patch("orchestra_dbt.cli.construct_dag")
    @patch("orchestra_dbt.cli.calculate_models_to_run")
    @patch("orchestra_dbt.cli.run_dbt_command")
    def test_dbt_command_full_flow(
        self,
        mock_run_dbt,
        mock_calc_models,
        mock_construct_dag,
        mock_load_state,
        mock_get_freshness,
        mock_validate,
    ):
        # Setup mocks
        mock_load_state.return_value = StateApiModel(state={})
        mock_get_freshness.return_value = Mock(sources={})
        mock_dag = ParsedDag(
            nodes={
                "model.test": Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.MODEL,
                    sql_path="models/test.sql",
                )
            },
            edges=[],
        )
        mock_construct_dag.return_value = mock_dag
        mock_calc_models.return_value = mock_dag

        runner = CliRunner()
        runner.invoke(dbt, ["run"])

        # Verify all steps were called
        mock_validate.assert_called_once()
        mock_get_freshness.assert_called_once()
        mock_load_state.assert_called_once()
        mock_construct_dag.assert_called_once()
        mock_calc_models.assert_called_once()
        mock_run_dbt.assert_called_once()
