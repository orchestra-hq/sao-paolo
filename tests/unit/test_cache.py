from datetime import datetime
from unittest.mock import Mock, patch

import httpx

from src.orchestra_dbt.cache import load_state, save_state
from src.orchestra_dbt.models import StateApiModel, StateItem


class TestLoadState:
    @patch("orchestra_dbt.cache.httpx.get")
    @patch("orchestra_dbt.cache.os.getenv")
    def test_load_state_success(self, mock_getenv, mock_httpx_get):
        mock_getenv.return_value = "test-cache-key"
        mock_response = Mock()
        mock_response.json.return_value = {
            "state": {
                "model.test": {
                    "last_updated": "2024-01-01T12:00:00",
                    "checksum": "abc123",
                }
            }
        }
        mock_response.raise_for_status = Mock()
        mock_httpx_get.return_value = mock_response

        result = load_state()

        assert isinstance(result, StateApiModel)
        assert "model.test" in result.state
        mock_httpx_get.assert_called_once()
        mock_response.raise_for_status.assert_called_once()

    @patch("orchestra_dbt.cache.httpx.get")
    @patch("orchestra_dbt.cache.log_warn")
    def test_load_state_http_error(self, mock_log_warn, mock_httpx_get):
        """Test handling of HTTP errors when loading state."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=Mock(), response=mock_response
        )
        mock_httpx_get.return_value = mock_response

        result = load_state()

        assert isinstance(result, StateApiModel)
        assert result.state == {}
        mock_log_warn.assert_called_once()

    @patch("orchestra_dbt.cache.httpx.get")
    @patch("orchestra_dbt.cache.log_error")
    def test_load_state_validation_error(self, mock_log_error, mock_httpx_get):
        """Test handling of validation errors when loading state."""
        mock_response = Mock()
        mock_response.json.return_value = {"invalid": "data"}
        mock_response.raise_for_status = Mock()
        mock_httpx_get.return_value = mock_response

        result = load_state()

        assert isinstance(result, StateApiModel)
        assert result.state == {}
        mock_log_error.assert_called_once()

    @patch("orchestra_dbt.cache.httpx.get")
    def test_load_state_uses_env_var(self, mock_httpx_get):
        """Test that load_state uses ORCHESTRA_DBT_CACHE_KEY environment variable."""
        with patch("orchestra_dbt.cache.os.getenv", return_value="custom-key"):
            mock_response = Mock()
            mock_response.json.return_value = {"state": {}}
            mock_response.raise_for_status = Mock()
            mock_httpx_get.return_value = mock_response

            load_state()

            call_args = mock_httpx_get.call_args
            assert "custom-key" in str(call_args)


class TestSaveState:
    """Tests for save_state function."""

    @patch("orchestra_dbt.cache.httpx.patch")
    @patch("orchestra_dbt.cache.os.getenv")
    def test_save_state_success(self, mock_getenv, mock_httpx_patch):
        """Test successful state saving."""
        mock_getenv.return_value = "test-cache-key"
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_httpx_patch.return_value = mock_response

        state = StateApiModel(
            state={
                "model.test": StateItem(
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="abc123",
                )
            }
        )

        save_state(state)

        mock_httpx_patch.assert_called_once()
        mock_response.raise_for_status.assert_called_once()
        call_kwargs = mock_httpx_patch.call_args.kwargs
        assert "json" in call_kwargs
        assert "headers" in call_kwargs

    @patch("orchestra_dbt.cache.httpx.patch")
    @patch("orchestra_dbt.cache.log_error")
    def test_save_state_http_error(self, mock_log_error, mock_httpx_patch):
        """Test handling of HTTP errors when saving state."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error", request=Mock(), response=mock_response
        )
        mock_httpx_patch.return_value = mock_response

        state = StateApiModel(state={})
        save_state(state)

        mock_log_error.assert_called_once()

    @patch("orchestra_dbt.cache.httpx.patch")
    def test_save_state_uses_env_var(self, mock_httpx_patch):
        """Test that save_state uses ORCHESTRA_DBT_CACHE_KEY environment variable."""
        with patch("orchestra_dbt.cache.os.getenv", return_value="custom-key"):
            mock_response = Mock()
            mock_response.raise_for_status = Mock()
            mock_httpx_patch.return_value = mock_response

            state = StateApiModel(state={})
            save_state(state)

            call_args = mock_httpx_patch.call_args
            assert "custom-key" in str(call_args)
