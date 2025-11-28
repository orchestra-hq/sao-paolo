import json
import os
from unittest.mock import patch

import pytest

from src.orchestra_dbt.utils import (
    BASE_API_URL,
    HEADERS,
    load_file,
    log_error,
    log_info,
    log_success,
    log_warn,
    modify_dbt_command,
    validate_environment,
)


class TestLogging:
    """Tests for logging functions."""

    @patch("orchestra_dbt.utils.click.echo")
    @patch("orchestra_dbt.utils.click.style")
    def test_log_info(self, mock_style, mock_echo):
        """Test log_info function."""
        mock_style.return_value = "styled message"
        log_info("test message")
        mock_style.assert_called_once()
        mock_echo.assert_called_once()

    @patch("orchestra_dbt.utils.click.echo")
    @patch("orchestra_dbt.utils.click.style")
    def test_log_success(self, mock_style, mock_echo):
        """Test log_success function."""
        mock_style.return_value = "styled message"
        log_success("test message")
        mock_style.assert_called_once()
        mock_echo.assert_called_once()

    @patch("orchestra_dbt.utils.click.echo")
    @patch("orchestra_dbt.utils.click.style")
    def test_log_warn(self, mock_style, mock_echo):
        """Test log_warn function."""
        mock_style.return_value = "styled message"
        log_warn("test message")
        mock_style.assert_called_once()
        mock_echo.assert_called_once()

    @patch("orchestra_dbt.utils.click.echo")
    @patch("orchestra_dbt.utils.click.style")
    def test_log_error(self, mock_style, mock_echo):
        """Test log_error function."""
        mock_style.return_value = "styled message"
        log_error("test message")
        mock_style.assert_called_once()
        mock_echo.assert_called_once()
        # Error messages should be prefixed with "ERROR:"
        call_args = str(mock_style.call_args)
        assert "ERROR:" in call_args


class TestLoadFile:
    """Tests for load_file function."""

    def test_load_file_success(self, tmp_path):
        """Test successful file loading."""
        test_file = tmp_path / "test.json"
        test_data = {"key": "value", "number": 42}
        test_file.write_text(json.dumps(test_data))

        result = load_file(str(test_file))

        assert result == test_data

    def test_load_file_not_found(self):
        """Test that loading non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_file("nonexistent_file.json")

    def test_load_file_invalid_json(self, tmp_path):
        """Test that loading invalid JSON raises JSONDecodeError."""
        test_file = tmp_path / "invalid.json"
        test_file.write_text("{ invalid json }")

        with pytest.raises(json.JSONDecodeError):
            load_file(str(test_file))


class TestModifyDbtCommand:
    """Tests for modify_dbt_command function."""

    def test_modify_dbt_command_adds_exclude(self):
        """Test that modify_dbt_command adds --exclude tag:reuse."""
        original_cmd = ["run", "--select", "model_a"]
        result = modify_dbt_command(original_cmd)

        assert "--exclude" in result
        assert "tag:reuse" in result
        assert result[-2:] == ["--exclude", "tag:reuse"]

    def test_modify_dbt_command_preserves_original(self):
        """Test that modify_dbt_command preserves original command parts."""
        original_cmd = ["run", "--select", "model_a"]
        result = modify_dbt_command(original_cmd)

        assert "run" in result
        assert "--select" in result
        assert "model_a" in result


class TestValidateEnvironment:
    """Tests for validate_environment function."""

    @patch("orchestra_dbt.utils.log_error")
    @patch("orchestra_dbt.utils.sys.exit")
    def test_validate_environment_missing_api_key(self, mock_exit, mock_log_error):
        """Test that missing ORCHESTRA_API_KEY causes exit."""
        with patch.dict(os.environ, {}, clear=True):
            validate_environment()
            mock_exit.assert_called_once_with(2)
            mock_log_error.assert_called()

    @patch("orchestra_dbt.utils.log_error")
    @patch("orchestra_dbt.utils.sys.exit")
    def test_validate_environment_missing_cache_key(self, mock_exit, mock_log_error):
        """Test that missing ORCHESTRA_DBT_CACHE_KEY causes exit."""
        with patch.dict(os.environ, {"ORCHESTRA_API_KEY": "test-key"}, clear=True):
            validate_environment()
            mock_exit.assert_called_once_with(2)
            mock_log_error.assert_called()

    @patch("orchestra_dbt.utils.log_error")
    @patch("orchestra_dbt.utils.sys.exit")
    def test_validate_environment_invalid_env(self, mock_exit, mock_log_error):
        """Test that invalid ORCHESTRA_ENV causes exit."""
        with patch.dict(
            os.environ,
            {
                "ORCHESTRA_API_KEY": "test-key",
                "ORCHESTRA_DBT_CACHE_KEY": "test-cache",
                "ORCHESTRA_ENV": "invalid",
            },
            clear=True,
        ):
            validate_environment()
            mock_exit.assert_called_once_with(2)
            mock_log_error.assert_called()

    @patch("orchestra_dbt.utils.log_info")
    def test_validate_environment_success(self, mock_log_info):
        """Test successful environment validation."""
        with patch.dict(
            os.environ,
            {
                "ORCHESTRA_API_KEY": "test-key",
                "ORCHESTRA_DBT_CACHE_KEY": "test-cache",
                "ORCHESTRA_ENV": "app",
            },
            clear=True,
        ):
            validate_environment()
            mock_log_info.assert_called_once()

    @patch("orchestra_dbt.utils.log_info")
    def test_validate_environment_valid_envs(self, mock_log_info):
        """Test that all valid environments pass validation."""
        valid_envs = ["app", "stage", "dev"]

        for env in valid_envs:
            with patch.dict(
                os.environ,
                {
                    "ORCHESTRA_API_KEY": "test-key",
                    "ORCHESTRA_DBT_CACHE_KEY": "test-cache",
                    "ORCHESTRA_ENV": env,
                },
                clear=True,
            ):
                validate_environment()
                mock_log_info.reset_mock()


class TestConstants:
    """Tests for module constants."""

    def test_base_api_url_format(self):
        """Test that BASE_API_URL has correct format."""
        assert BASE_API_URL.startswith("https://")
        assert "getorchestra.io" in BASE_API_URL

    def test_base_api_url_uses_env(self):
        """Test that BASE_API_URL uses ORCHESTRA_ENV."""
        with patch.dict(os.environ, {"ORCHESTRA_ENV": "dev"}, clear=True):
            from src.orchestra_dbt.utils import BASE_API_URL

            assert "dev.getorchestra.io" in BASE_API_URL

    def test_headers_structure(self):
        """Test that HEADERS has correct structure."""
        assert isinstance(HEADERS, dict)
        assert "Authorization" in HEADERS
