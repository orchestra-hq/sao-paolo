"""
Shared pytest fixtures and configuration for all tests.
"""

import os
from datetime import datetime
from unittest.mock import Mock

import httpx
import pytest

from orchestra_dbt.models import (
    SourceFreshness,
    StateApiModel,
    StateItem,
)


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Fixture to set common environment variables for testing."""
    monkeypatch.setenv("ORCHESTRA_API_KEY", "test-api-key")
    monkeypatch.setenv("ORCHESTRA_DBT_CACHE_KEY", "test-cache-key")
    monkeypatch.setenv("ORCHESTRA_ENV", "app")
    yield
    # Cleanup is automatic with monkeypatch


@pytest.fixture
def sample_state_item():
    """Fixture providing a sample StateItem."""
    return StateItem(
        last_updated=datetime(2024, 1, 1, 12, 0, 0),
        checksum="abc123",
    )


@pytest.fixture
def sample_state_api_model(sample_state_item):
    """Fixture providing a sample StateApiModel."""
    return StateApiModel(
        state={
            "source.test_db.test_schema.test_table": sample_state_item,
            "model.test_project.model_a": StateItem(
                last_updated=datetime(2024, 1, 2, 12, 0, 0),
                checksum="def456",
            ),
        }
    )


@pytest.fixture
def sample_source_freshness():
    """Fixture providing a sample SourceFreshness."""
    return SourceFreshness(
        sources={
            "source.test_db.test_schema.test_table": datetime(2024, 1, 3, 12, 0, 0),
        }
    )


@pytest.fixture
def sample_manifest():
    """Fixture providing a sample dbt manifest.json structure."""
    return {
        "nodes": {
            "model.test_project.model_a": {
                "resource_type": "model",
                "checksum": {"checksum": "def456"},
                "config": {"freshness": None},
                "original_file_path": "models/model_a.sql",
                "depends_on": {
                    "nodes": ["source.test_db.test_schema.test_table"],
                },
            },
            "model.test_project.model_b": {
                "resource_type": "model",
                "checksum": {"checksum": "ghi789"},
                "config": {"freshness": None},
                "original_file_path": "models/model_b.sql",
                "depends_on": {
                    "nodes": ["model.test_project.model_a"],
                },
            },
        },
        "child_map": {
            "source.test_db.test_schema.test_table": [
                "model.test_project.model_a",
            ],
        },
    }


@pytest.fixture
def sample_sources_json():
    """Fixture providing a sample sources.json structure."""
    return {
        "results": [
            {
                "unique_id": "source.test_db.test_schema.test_table",
                "max_loaded_at": "2024-01-03T12:00:00",
            },
        ]
    }


@pytest.fixture
def mock_httpx_response(monkeypatch):
    """Fixture to mock httpx responses."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.json.return_value = {"state": {}}
    mock_response.raise_for_status = Mock()

    def mock_get(*args, **kwargs):
        return mock_response

    def mock_patch(*args, **kwargs):
        return mock_response

    monkeypatch.setattr(httpx, "get", mock_get)
    monkeypatch.setattr(httpx, "patch", mock_patch)

    return mock_response


@pytest.fixture
def temp_dir(tmp_path):
    """Fixture providing a temporary directory for file operations."""
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(original_cwd)


@pytest.fixture
def sample_sql_file_content():
    """Fixture providing sample SQL file content."""
    return """-- Some SQL model
SELECT
    id,
    name,
    created_at
FROM {{ ref('some_model') }}
"""
