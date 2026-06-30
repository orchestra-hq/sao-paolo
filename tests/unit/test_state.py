from datetime import datetime
from unittest.mock import MagicMock, patch

import boto3
import httpx
import pytest
from cloud_storage_mocker import Mount
from cloud_storage_mocker import patch as gcs_patch
from moto import mock_aws
from pytest_httpx import HTTPXMock

from src.orchestra_dbt.models import (
    Edge,
    Freshness,
    FreshnessConfig,
    MaterialisationNode,
    ParsedDag,
    SourceFreshness,
    SourceNode,
    StateApiModel,
    StateItem,
)
from src.orchestra_dbt.state import (
    StateLoadError,
    StateSaveError,
    _load_run_results,
    load_state,
    save_state,
    update_state,
)


class TestLoadState:
    @patch("src.orchestra_dbt.state_filters.get_integration_account_id")
    @pytest.mark.parametrize(
        "integration_account_id, expected_state_len",
        [(None, 3), ("a", 1), ("b", 1)],
    )
    def test_load_state_success(
        self,
        mock_get_integration_account_id,
        httpx_mock: HTTPXMock,
        integration_account_id: str | None,
        expected_state_len: int,
    ):
        httpx_mock.add_response(
            url="https://dev.getorchestra.io/api/engine/public/state/DBT_CORE",
            match_headers={
                "Accept": "application/json",
                "Authorization": "Bearer test-api-key",
            },
            json={
                "state": {
                    "a.model.test": {
                        "checksum": "123",
                        "last_updated": "2024-01-01T12:00:00",
                        "sources": {
                            "source.test": "2024-01-01T11:00:00",
                        },
                    },
                    "b.model.test": {
                        "checksum": "123",
                        "last_updated": "2024-01-01T12:00:00",
                        "sources": {
                            "source.test": "2024-01-01T11:00:00",
                        },
                    },
                    "model.test": {
                        "checksum": "123",
                        "last_updated": "2024-01-01T12:00:00",
                        "sources": {
                            "source.test": "2024-01-01T11:00:00",
                        },
                    },
                }
            },
        )

        mock_get_integration_account_id.return_value = integration_account_id
        loaded_state = load_state()
        assert len(loaded_state.state) == expected_state_len
        assert list(loaded_state.state.values())[0] == StateItem(
            last_updated=datetime(2024, 1, 1, 12, 0, 0),
            checksum="123",
            sources={
                "source.test": datetime(2024, 1, 1, 11, 0, 0),
            },
        )

    def test_load_state_http_error(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            url="https://dev.getorchestra.io/api/engine/public/state/DBT_CORE",
            match_headers={
                "Accept": "application/json",
                "Authorization": "Bearer test-api-key",
            },
            status_code=400,
        )
        assert load_state() == StateApiModel(state={})

    def test_load_state_validation_error(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            url="https://dev.getorchestra.io/api/engine/public/state/DBT_CORE",
            match_headers={
                "Accept": "application/json",
                "Authorization": "Bearer test-api-key",
            },
            json={"invalid": "data"},
        )
        assert load_state() == StateApiModel(state={})

    def test_load_state_request_error(self, httpx_mock: HTTPXMock):
        httpx_mock.add_exception(
            httpx.ConnectError("Connection failed"),
            method="GET",
            url="https://dev.getorchestra.io/api/engine/public/state/DBT_CORE",
            match_headers={
                "Accept": "application/json",
                "Authorization": "Bearer test-api-key",
            },
        )
        assert load_state() == StateApiModel(state={})


class TestSaveState:
    def test_save_state_success(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="PATCH",
            url="https://dev.getorchestra.io/api/engine/public/state/DBT_CORE",
            match_headers={
                "Authorization": "Bearer test-api-key",
                "Content-Type": "application/json",
            },
            match_json={
                "state": {
                    "model.test": {
                        "last_updated": "2024-01-01T14:00:00",
                        "checksum": "123",
                        "sources": {
                            "source.test": "2024-01-01T11:00:00",
                        },
                    },
                    "model.new": {
                        "last_updated": "2024-01-01T14:00:00",
                        "checksum": "456",
                        "sources": {
                            "source.test": "2024-01-01T11:00:00",
                        },
                    },
                }
            },
        )
        assert (
            save_state(
                state=StateApiModel(
                    state={
                        "model.test": StateItem(
                            last_updated=datetime(2024, 1, 1, 14, 0, 0),
                            checksum="123",
                            sources={
                                "source.test": datetime(2024, 1, 1, 11, 0, 0),
                            },
                        ),
                        "model.new": StateItem(
                            last_updated=datetime(2024, 1, 1, 14, 0, 0),
                            checksum="456",
                            sources={
                                "source.test": datetime(2024, 1, 1, 11, 0, 0),
                            },
                        ),
                    }
                )
            )
            is None
        )

    def test_save_state_http_error(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="PATCH",
            url="https://dev.getorchestra.io/api/engine/public/state/DBT_CORE",
            match_headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer test-api-key",
            },
            status_code=500,
        )
        assert save_state(state=StateApiModel(state={})) is None

    def test_save_state_timeout(self, httpx_mock: HTTPXMock):
        httpx_mock.add_exception(
            httpx.TimeoutException("Request timed out"),
            method="PATCH",
            url="https://dev.getorchestra.io/api/engine/public/state/DBT_CORE",
            match_headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer test-api-key",
            },
        )
        assert save_state(state=StateApiModel(state={})) is None


class TestUpdateState:
    @pytest.fixture(autouse=True)
    def clear_lru_cache_run_results(self):
        yield
        _load_run_results.cache_clear()

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_with_model_and_run_results(self, mock_load_json):
        """Test updating state with a model node that has successful run results."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_a",
                    "status": "success",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T10:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T12:00:00"},
                    ],
                }
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                )
            },
            edges=[],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert "model.test_project.model_a" in state.state
        assert state.state["model.test_project.model_a"].checksum == "abc123"
        assert state.state["model.test_project.model_a"].last_updated == datetime(
            2024, 1, 1, 12, 0, 0
        )
        assert state.state["model.test_project.model_a"].sources == {}

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_with_source_parents(self, mock_load_json):
        """Test updating state with a model that has source parents."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_a",
                    "status": "success",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T10:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T12:00:00"},
                    ],
                }
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "source.test_db.test_schema.test_table": SourceNode(),
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
            },
            edges=[
                Edge(
                    from_="source.test_db.test_schema.test_table",
                    to_="model.test_project.model_a",
                )
            ],
        )
        source_freshness = SourceFreshness(
            sources={
                "source.test_db.test_schema.test_table": datetime(2024, 1, 1, 11, 0, 0)
            }
        )

        update_state(state, parsed_dag, source_freshness)

        assert "model.test_project.model_a" in state.state
        assert state.state["model.test_project.model_a"].sources == {
            "source.test_db.test_schema.test_table": datetime(2024, 1, 1, 11, 0, 0)
        }

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_skips_model_without_run_results(self, mock_load_json):
        """Test that models without run results are skipped."""
        mock_load_json.return_value = {"results": []}

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="integration_account_id.model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                )
            },
            edges=[],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert state.state == {}

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_skips_model_with_failed_status(self, mock_load_json):
        """Test that models with failed status are skipped."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_a",
                    "status": "error",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T10:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T12:00:00"},
                    ],
                }
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="integration_account_id.model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                )
            },
            edges=[],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert state.state == {}

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_skips_non_model_nodes(self, mock_load_json):
        """Test that non-model nodes (like sources) are skipped."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "source.test_db.test_schema.test_table",
                    "status": "success",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T10:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T12:00:00"},
                    ],
                }
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "source.test_db.test_schema.test_table": SourceNode(),
            },
            edges=[],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert state.state == {}

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_with_multiple_models(self, mock_load_json):
        """Test updating state with multiple model nodes."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_a",
                    "status": "success",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T10:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T12:00:00"},
                    ],
                },
                {
                    "unique_id": "model.test_project.model_b",
                    "status": "success",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T11:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T13:00:00"},
                    ],
                },
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                "model.test_project.model_b": MaterialisationNode(
                    asset_external_id="model.test_project.model_b",
                    checksum="def456",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_b.sql",
                    file_path="models/model_b.sql",
                    sources={},
                    reason="Node not seen before",
                    freshness_config=FreshnessConfig(),
                ),
            },
            edges=[],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert len(state.state) == 2
        assert "model.test_project.model_a" in state.state
        assert "model.test_project.model_b" in state.state
        assert state.state["model.test_project.model_a"].checksum == "abc123"
        assert state.state["model.test_project.model_b"].checksum == "def456"

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_with_multiple_source_parents(self, mock_load_json):
        """Test updating state with a model that has multiple source parents."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_a",
                    "status": "success",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T10:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T12:00:00"},
                    ],
                }
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "source.test_db.test_schema.table1": SourceNode(),
                "source.test_db.test_schema.table2": SourceNode(),
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
            },
            edges=[
                Edge(
                    from_="source.test_db.test_schema.table1",
                    to_="model.test_project.model_a",
                ),
                Edge(
                    from_="source.test_db.test_schema.table2",
                    to_="model.test_project.model_a",
                ),
            ],
        )
        source_freshness = SourceFreshness(
            sources={
                "source.test_db.test_schema.table1": datetime(2024, 1, 1, 11, 0, 0),
                "source.test_db.test_schema.table2": datetime(2024, 1, 1, 11, 30, 0),
            }
        )

        update_state(state, parsed_dag, source_freshness)

        assert "model.test_project.model_a" in state.state
        assert len(state.state["model.test_project.model_a"].sources) == 2
        assert (
            "source.test_db.test_schema.table1"
            in state.state["model.test_project.model_a"].sources
        )
        assert (
            "source.test_db.test_schema.table2"
            in state.state["model.test_project.model_a"].sources
        )

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_skips_source_not_in_freshness(self, mock_load_json):
        """Test that source parents not in source_freshness are skipped."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_a",
                    "status": "success",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T10:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T12:00:00"},
                    ],
                }
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "source.test_db.test_schema.table1": SourceNode(),
                "source.test_db.test_schema.table2": SourceNode(),
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
            },
            edges=[
                Edge(
                    from_="source.test_db.test_schema.table1",
                    to_="model.test_project.model_a",
                ),
                Edge(
                    from_="source.test_db.test_schema.table2",
                    to_="model.test_project.model_a",
                ),
            ],
        )
        # Only table1 is in source_freshness
        source_freshness = SourceFreshness(
            sources={
                "source.test_db.test_schema.table1": datetime(2024, 1, 1, 11, 0, 0)
            }
        )

        update_state(state, parsed_dag, source_freshness)

        assert "model.test_project.model_a" in state.state
        # Only table1 should be included since table2 is not in source_freshness
        assert len(state.state["model.test_project.model_a"].sources) == 1
        assert (
            "source.test_db.test_schema.table1"
            in state.state["model.test_project.model_a"].sources
        )
        assert (
            "source.test_db.test_schema.table2"
            not in state.state["model.test_project.model_a"].sources
        )

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_overwrites_existing_state(self, mock_load_json):
        """Test that update_state overwrites existing state entries."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_a",
                    "status": "success",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T10:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T14:00:00"},
                    ],
                }
            ]
        }

        # Start with existing state
        state = StateApiModel(
            state={
                "model.test_project.model_a": StateItem(
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="old_checksum",
                    sources={},
                )
            }
        )
        parsed_dag = ParsedDag(
            nodes={
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="model.test_project.model_a",
                    checksum="new_checksum",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                )
            },
            edges=[],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert state.state["model.test_project.model_a"].checksum == "new_checksum"
        assert state.state["model.test_project.model_a"].last_updated == datetime(
            2024, 1, 1, 14, 0, 0
        )

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_skips_model_parents(self, mock_load_json):
        """Test that model parents (not sources) are not included in sources dict."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_b",
                    "status": "success",
                    "timing": [
                        {"name": "compile", "started_at": "2024-01-01T10:00:00"},
                        {"name": "execute", "completed_at": "2024-01-01T12:00:00"},
                    ],
                }
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
                "model.test_project.model_b": MaterialisationNode(
                    asset_external_id="model.test_project.model_b",
                    checksum="def456",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_b.sql",
                    file_path="models/model_b.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                ),
            },
            edges=[
                Edge(
                    from_="model.test_project.model_a",
                    to_="model.test_project.model_b",
                )
            ],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert "model.test_project.model_b" in state.state
        # model_a is a parent but not a source, so it shouldn't be in sources dict
        assert state.state["model.test_project.model_b"].sources == {}

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_handles_missing_timing(self, mock_load_json):
        """Test that models with missing timing data are skipped."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_a",
                    "status": "success",
                    # Missing timing field
                }
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="integration_account_id.model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                )
            },
            edges=[],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert state.state == {}

    @patch("src.orchestra_dbt.state.load_json")
    def test_update_state_handles_empty_timing(self, mock_load_json):
        """Test that models with empty timing array are skipped."""
        mock_load_json.return_value = {
            "results": [
                {
                    "unique_id": "model.test_project.model_a",
                    "status": "success",
                    "timing": [],
                }
            ]
        }

        state = StateApiModel(state={})
        parsed_dag = ParsedDag(
            nodes={
                "model.test_project.model_a": MaterialisationNode(
                    asset_external_id="model.test_project.model_a",
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    dbt_path="models/model_a.sql",
                    file_path="models/model_a.sql",
                    reason="Node not seen before",
                    sources={},
                    freshness_config=FreshnessConfig(),
                )
            },
            edges=[],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert state.state == {}


class TestLoadStateFile:
    def test_load_state_file_missing(self, monkeypatch: pytest.MonkeyPatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", str(tmp_path / "missing.json"))

        with pytest.raises(StateLoadError, match="State file not found"):
            load_state()

    def test_load_state_file_empty_state(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ):
        p = tmp_path / "st.json"
        p.write_text('{"state": {}}', encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", str(p))

        assert load_state() == StateApiModel(state={})

    def test_load_state_file_invalid_json(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ):
        p = tmp_path / "st.json"
        p.write_text("not json", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", str(p))

        with pytest.raises(StateLoadError, match="not valid JSON"):
            load_state()


class TestSaveStateFile:
    def test_save_state_file_round_trip(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ):
        p = tmp_path / "st.json"
        p.write_text('{"state": {}}', encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", str(p))
        state = StateApiModel(
            state={
                "model.test": StateItem(
                    last_updated=datetime(2024, 1, 1, 14, 0, 0),
                    checksum="123",
                    sources={"source.test": datetime(2024, 1, 1, 11, 0, 0)},
                )
            }
        )
        save_state(state)
        loaded = load_state()
        assert loaded.state["model.test"].checksum == "123"


class TestLoadStateS3:
    @mock_aws
    def test_load_state_s3_missing_object_starts_empty(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ):
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", "s3://test-bucket/prefix/state.json")

        assert load_state() == StateApiModel(state={})

    @mock_aws
    def test_load_state_s3_success(self, monkeypatch: pytest.MonkeyPatch, tmp_path):
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket-s3")
        payload = (
            b'{"state": {"model.x": {"checksum": "c", '
            b'"last_updated": "2024-01-01T12:00:00", "sources": {}}}}'
        )
        conn.put_object(Bucket="test-bucket-s3", Key="k.json", Body=payload)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", "s3://test-bucket-s3/k.json")

        loaded = load_state()
        assert "model.x" in loaded.state
        assert loaded.state["model.x"].checksum == "c"


class TestSaveStateS3:
    @mock_aws
    def test_save_state_s3_put_object(self, monkeypatch: pytest.MonkeyPatch, tmp_path):
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", "s3://bucket/dir/f.json")

        save_state(
            StateApiModel(
                state={
                    "m": StateItem(
                        last_updated=datetime(2024, 1, 1, 12, 0, 0),
                        checksum="1",
                        sources={},
                    )
                }
            )
        )

        obj = conn.get_object(Bucket="bucket", Key="dir/f.json")
        body = obj["Body"].read()
        assert b'"checksum"' in body


class TestLoadStateGCS:
    def test_load_state_gcs_missing_blob_starts_empty(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", "gs://test-bucket/prefix/state.json")

        # cloud-storage-mocker doesn't implement get_bucket; patch it to confirm
        # the bucket exists so the missing-blob path is exercised.
        from cloud_storage_mocker._core import Client as MockClient

        with gcs_patch(
            mounts=[Mount("test-bucket", tmp_path / "gcs", readable=True, writable=True)]
        ):
            with patch.object(MockClient, "get_bucket", return_value=None, create=True):
                assert load_state() == StateApiModel(state={})

    def test_load_state_gcs_success(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ):
        bucket_dir = tmp_path / "gcs"
        blob_path = bucket_dir / "k.json"
        bucket_dir.mkdir()
        blob_path.write_text(
            '{"state": {"model.x": {"checksum": "c", '
            '"last_updated": "2024-01-01T12:00:00", "sources": {}}}}'
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", "gs://test-bucket/k.json")

        with gcs_patch(
            mounts=[Mount("test-bucket", bucket_dir, readable=True, writable=True)]
        ):
            loaded = load_state()

        assert "model.x" in loaded.state
        assert loaded.state["model.x"].checksum == "c"

    def test_load_state_gcs_credential_error_raises_state_load_error(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", "gs://bucket/state.json")

        from google.auth.exceptions import DefaultCredentialsError

        with patch(
            "orchestra_dbt.state_backends.gcs.storage.Client",
            side_effect=DefaultCredentialsError("no credentials"),
        ):
            with pytest.raises(StateLoadError):
                load_state()


class TestSaveStateGCS:
    def test_save_state_gcs_uploads_blob(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ):
        bucket_dir = tmp_path / "gcs"
        bucket_dir.mkdir()
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", "gs://bucket/dir/f.json")

        with gcs_patch(
            mounts=[Mount("bucket", bucket_dir, readable=True, writable=True)]
        ):
            save_state(
                StateApiModel(
                    state={
                        "m": StateItem(
                            last_updated=datetime(2024, 1, 1, 12, 0, 0),
                            checksum="1",
                            sources={},
                        )
                    }
                )
            )

        body = (bucket_dir / "dir" / "f.json").read_text()
        assert '"checksum"' in body

    def test_save_state_gcs_credential_error_raises_state_save_error(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
        monkeypatch.setenv("ORCHESTRA_STATE_FILE", "gs://bucket/state.json")

        from google.auth.exceptions import DefaultCredentialsError

        with patch(
            "orchestra_dbt.state_backends.gcs.storage.Client",
            side_effect=DefaultCredentialsError("no credentials"),
        ):
            with pytest.raises(StateSaveError):
                save_state(StateApiModel(state={}))


class TestAzureStateBackend:
    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    @patch("src.orchestra_dbt.state_backends.azure.DefaultAzureCredential")
    def test_load_returns_empty_state_when_blob_missing(
        self, mock_credential, mock_client_cls
    ):
        from azure.core.exceptions import ResourceNotFoundError

        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.side_effect = ResourceNotFoundError("not found")
        mock_container_client = MagicMock()
        mock_container_client.exists.return_value = True
        mock_service = MagicMock()
        mock_service.get_blob_client.return_value = mock_blob_client
        mock_service.get_container_client.return_value = mock_container_client
        mock_client_cls.return_value = mock_service

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        result = backend.load()

        assert result == StateApiModel(state={})

    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    @patch("src.orchestra_dbt.state_backends.azure.DefaultAzureCredential")
    def test_load_raises_when_container_missing(
        self, mock_credential, mock_client_cls
    ):
        from azure.core.exceptions import ResourceNotFoundError
        from src.orchestra_dbt.state_errors import StateLoadError

        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.side_effect = ResourceNotFoundError("not found")
        mock_container_client = MagicMock()
        mock_container_client.exists.return_value = False
        mock_service = MagicMock()
        mock_service.get_blob_client.return_value = mock_blob_client
        mock_service.get_container_client.return_value = mock_container_client
        mock_client_cls.return_value = mock_service

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        with pytest.raises(StateLoadError, match="container"):
            backend.load()

    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    @patch("src.orchestra_dbt.state_backends.azure.DefaultAzureCredential")
    def test_load_returns_valid_state(self, mock_credential, mock_client_cls):
        payload = '{"state": {"model.test": {"checksum": "abc", "last_updated": "2024-01-01T12:00:00", "sources": {}}}}'
        mock_download = MagicMock()
        mock_download.readall.return_value = payload.encode("utf-8")
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value = mock_download
        mock_service = MagicMock()
        mock_service.get_blob_client.return_value = mock_blob_client
        mock_client_cls.return_value = mock_service

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        result = backend.load()

        assert "model.test" in result.state
        assert result.state["model.test"].checksum == "abc"

    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    @patch("src.orchestra_dbt.state_backends.azure.DefaultAzureCredential")
    def test_save_uploads_json(self, mock_credential, mock_client_cls):
        mock_blob_client = MagicMock()
        mock_service = MagicMock()
        mock_service.get_blob_client.return_value = mock_blob_client
        mock_client_cls.return_value = mock_service

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        backend.save(StateApiModel(state={}))

        mock_blob_client.upload_blob.assert_called_once()
        call_kwargs = mock_blob_client.upload_blob.call_args
        assert call_kwargs.kwargs.get("overwrite") is True
        assert call_kwargs.kwargs.get("content_settings") is not None

    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    @patch("src.orchestra_dbt.state_backends.azure.DefaultAzureCredential")
    def test_save_raises_on_auth_error(self, mock_credential, mock_client_cls):
        from azure.core.exceptions import HttpResponseError
        from src.orchestra_dbt.state_errors import StateSaveError

        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob.side_effect = HttpResponseError(
            message="AuthorizationFailure"
        )
        mock_service = MagicMock()
        mock_service.get_blob_client.return_value = mock_blob_client
        mock_client_cls.return_value = mock_service

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        with pytest.raises(StateSaveError):
            backend.save(StateApiModel(state={}))

    @patch.dict("os.environ", {"AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=fake;EndpointSuffix=core.windows.net"})
    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    def test_load_uses_connection_string_when_set(self, mock_client_cls):
        payload = '{"state": {}}'
        mock_download = MagicMock()
        mock_download.readall.return_value = payload.encode("utf-8")
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value = mock_download
        mock_service = MagicMock()
        mock_service.get_blob_client.return_value = mock_blob_client
        mock_client_cls.from_connection_string.return_value = mock_service

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        result = backend.load()

        mock_client_cls.from_connection_string.assert_called_once()
        assert result == StateApiModel(state={})

    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    @patch("src.orchestra_dbt.state_backends.azure.DefaultAzureCredential")
    def test_load_uses_default_credential_when_no_connection_string(
        self, mock_credential_cls, mock_client_cls
    ):
        payload = '{"state": {}}'
        mock_download = MagicMock()
        mock_download.readall.return_value = payload.encode("utf-8")
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value = mock_download
        mock_service = MagicMock()
        mock_service.get_blob_client.return_value = mock_blob_client
        mock_client_cls.return_value = mock_service

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        result = backend.load()

        mock_credential_cls.assert_called_once()
        mock_client_cls.assert_called_once()
        assert result == StateApiModel(state={})

    @patch.dict("os.environ", {"AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=otheraccount;AccountKey=fake;EndpointSuffix=core.windows.net"})
    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    def test_load_raises_when_connection_string_account_mismatches_uri(
        self, mock_client_cls
    ):
        from src.orchestra_dbt.state_errors import StateLoadError

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        with pytest.raises(StateLoadError, match="must match"):
            backend.load()

        mock_client_cls.from_connection_string.assert_not_called()

    @patch.dict("os.environ", {"AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=otheraccount;AccountKey=fake;EndpointSuffix=core.windows.net"})
    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    def test_save_raises_when_connection_string_account_mismatches_uri(
        self, mock_client_cls
    ):
        from src.orchestra_dbt.state_errors import StateSaveError

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        with pytest.raises(StateSaveError, match="must match"):
            backend.save(StateApiModel(state={}))

        mock_client_cls.from_connection_string.assert_not_called()

    @patch.dict("os.environ", {"AZURE_STORAGE_CONNECTION_STRING": "not-a-valid-connection-string"})
    @patch("src.orchestra_dbt.state_backends.azure.BlobServiceClient")
    def test_load_wraps_invalid_connection_string_as_state_load_error(
        self, mock_client_cls
    ):
        from src.orchestra_dbt.state_errors import StateLoadError

        mock_client_cls.from_connection_string.side_effect = ValueError(
            "Connection string missing required connection details."
        )

        from src.orchestra_dbt.state_backends.azure import AzureStateBackend

        # Account name is absent from the string, so the mismatch guard passes and
        # from_connection_string runs and raises — which must surface as StateLoadError.
        backend = AzureStateBackend("myaccount", "mycontainer", "state.json")
        with pytest.raises(StateLoadError, match="initialize Azure client"):
            backend.load()
