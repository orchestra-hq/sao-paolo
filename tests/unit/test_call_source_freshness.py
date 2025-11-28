from unittest.mock import Mock, patch

from src.orchestra_dbt.call_source_freshness import source_freshness_invoke


class TestSourceFreshnessInvoke:
    """Tests for source_freshness_invoke function."""

    @patch("orchestra_dbt.call_source_freshness.log_info")
    @patch("orchestra_dbt.call_source_freshness.dbtRunner")
    @patch("orchestra_dbt.call_source_freshness.SourceDefinition")
    def test_source_freshness_invoke_success(
        self, mock_source_def, mock_dbt_runner, mock_log_info
    ):
        """Test successful source freshness invocation."""
        mock_runner_instance = Mock()
        mock_runner_instance.invoke.return_value = Mock()
        mock_dbt_runner.return_value = mock_runner_instance

        source_freshness_invoke()

        mock_log_info.assert_called_once()
        assert mock_source_def.has_freshness is True
        mock_runner_instance.invoke.assert_called_once_with(
            ["source", "freshness", "-q"]
        )

    @patch("orchestra_dbt.call_source_freshness.log_warn")
    @patch("orchestra_dbt.call_source_freshness.log_info")
    @patch("orchestra_dbt.call_source_freshness.dbtRunner")
    @patch("orchestra_dbt.call_source_freshness.SourceDefinition")
    def test_source_freshness_invoke_handles_exception(
        self, mock_source_def, mock_dbt_runner, mock_log_info, mock_log_warn
    ):
        """Test that exceptions are handled gracefully."""
        mock_runner_instance = Mock()
        mock_runner_instance.invoke.side_effect = Exception("dbt error")
        mock_dbt_runner.return_value = mock_runner_instance

        source_freshness_invoke()

        mock_log_warn.assert_called_once()
        # Should still set has_freshness
        assert mock_source_def.has_freshness is True

    @patch("orchestra_dbt.call_source_freshness.dbtRunner")
    @patch("orchestra_dbt.call_source_freshness.SourceDefinition")
    def test_source_freshness_sets_has_freshness(
        self, mock_source_def, mock_dbt_runner
    ):
        """Test that SourceDefinition.has_freshness is set to True."""
        mock_runner_instance = Mock()
        mock_runner_instance.invoke.return_value = Mock()
        mock_dbt_runner.return_value = mock_runner_instance

        # Initially might be False or have some other value
        mock_source_def.has_freshness = False

        source_freshness_invoke()

        assert mock_source_def.has_freshness is True

    @patch("orchestra_dbt.call_source_freshness.log_info")
    @patch("orchestra_dbt.call_source_freshness.dbtRunner")
    @patch("orchestra_dbt.call_source_freshness.SourceDefinition")
    def test_source_freshness_calls_dbt_runner_correctly(
        self, mock_source_def, mock_dbt_runner, mock_log_info
    ):
        """Test that dbtRunner is called with correct arguments."""
        mock_runner_instance = Mock()
        mock_runner_instance.invoke.return_value = Mock()
        mock_dbt_runner.return_value = mock_runner_instance

        source_freshness_invoke()

        mock_dbt_runner.assert_called_once()
        mock_runner_instance.invoke.assert_called_once()
        invoke_args = mock_runner_instance.invoke.call_args[0][0]
        assert invoke_args == ["source", "freshness", "-q"]
