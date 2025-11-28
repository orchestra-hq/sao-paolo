from unittest.mock import Mock, patch

import pytest

from src.orchestra_dbt.dbt_runner import run_dbt_command


class TestRunDbtCommand:
    """Tests for run_dbt_command function."""

    @patch("orchestra_dbt.dbt_runner.subprocess.run")
    @patch("orchestra_dbt.dbt_runner.log_info")
    def test_run_dbt_command_success(self, mock_log_info, mock_subprocess_run):
        """Test successful dbt command execution."""
        mock_subprocess_run.return_value = Mock(returncode=0)

        run_dbt_command(["run", "--select", "model_a"])

        mock_subprocess_run.assert_called_once()
        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "dbt"
        assert "run" in call_args
        mock_log_info.assert_called_once()

    @patch("orchestra_dbt.dbt_runner.subprocess.run")
    @patch("orchestra_dbt.dbt_runner.log_info")
    def test_run_dbt_command_with_multiple_args(
        self, mock_log_info, mock_subprocess_run
    ):
        """Test dbt command with multiple arguments."""
        mock_subprocess_run.return_value = Mock(returncode=0)

        run_dbt_command(["run", "--select", "model_a", "--vars", "key:value"])

        call_args = mock_subprocess_run.call_args[0][0]
        assert len(call_args) == 5
        assert "--vars" in call_args
        assert "key:value" in call_args

    @patch("orchestra_dbt.dbt_runner.subprocess.run")
    @patch("orchestra_dbt.dbt_runner.log_info")
    def test_run_dbt_command_logs_command(self, mock_log_info, mock_subprocess_run):
        """Test that the command is logged before execution."""
        mock_subprocess_run.return_value = Mock(returncode=0)

        run_dbt_command(["run"])

        log_call = str(mock_log_info.call_args)
        assert "dbt" in log_call
        assert "run" in log_call

    @patch("orchestra_dbt.dbt_runner.subprocess.run")
    @patch("orchestra_dbt.dbt_runner.log_info")
    def test_run_dbt_command_handles_failure(self, mock_log_info, mock_subprocess_run):
        """Test that CalledProcessError is handled gracefully."""
        mock_subprocess_run.side_effect = Exception("Process failed")

        # Should not raise exception
        try:
            run_dbt_command(["run"])
        except Exception:
            pytest.fail("run_dbt_command should handle exceptions gracefully")

    @patch("orchestra_dbt.dbt_runner.subprocess.run")
    def test_run_dbt_command_calls_check(self, mock_subprocess_run):
        """Test that subprocess.run is called with check=True."""
        mock_subprocess_run.return_value = Mock(returncode=0)

        run_dbt_command(["run"])

        call_kwargs = mock_subprocess_run.call_args[1]
        assert call_kwargs.get("check") is True
