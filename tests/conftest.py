import pytest


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("ORCHESTRA_API_KEY", "test-api-key")
    monkeypatch.setenv("ORCHESTRA_ENV", "dev")
    yield


@pytest.fixture
def sample_manifest():
    return {
        "metadata": {
            "project_name": "test_project",
        },
        "nodes": {
            "model.test_project.model_a": {
                "resource_type": "model",
                "checksum": {"checksum": "def456"},
                "config": {"freshness": None},
                "package_name": "test_project",
                "original_file_path": "models/model_a.sql",
                "depends_on": {
                    "nodes": ["source.test_db.test_schema.test_table"],
                },
            },
            "model.test_project_2.model_b": {
                "resource_type": "model",
                "checksum": {"checksum": "ghi789"},
                "config": {"freshness": None},
                "package_name": "test_project_2",
                "original_file_path": "models/model_b.sql",
                "depends_on": {
                    "nodes": ["model.test_project.model_a"],
                },
            },
            "model.test_project.model_c": {
                "resource_type": "model",
                "checksum": {"checksum": "456"},
                "config": {"freshness": None},
                "package_name": "test_project",
                "original_file_path": "models/model_c.sql",
                "depends_on": {
                    "nodes": [],
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
    return {
        "results": [
            {
                "unique_id": "source.test_db.test_schema.test_table",
                "max_loaded_at": "2024-01-03T12:00:00",
            },
        ]
    }
