import os

import pytest


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("ORCHESTRA_API_KEY", "test-api-key")
    monkeypatch.setenv("ORCHESTRA_DBT_CACHE_KEY", "test-cache-key")
    monkeypatch.setenv("ORCHESTRA_ENV", "dev")
    yield


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
