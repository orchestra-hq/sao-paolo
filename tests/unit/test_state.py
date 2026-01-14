from datetime import datetime
from unittest.mock import patch

import pytest
from pytest_httpx import HTTPXMock

from src.orchestra_dbt.models import (
    Edge,
    Freshness,
    ModelNode,
    ParsedDag,
    SourceFreshness,
    SourceNode,
    StateApiModel,
    StateItem,
)
from src.orchestra_dbt.state import (
    _load_run_results,
    load_state,
    save_state,
    update_state,
)


class TestLoadState:
    @patch("src.orchestra_dbt.state.get_integration_account_id_from_env")
    @pytest.mark.parametrize(
        "integration_account_id, expected_state_len",
        [(None, 3), ("a", 1), ("b", 1)],
    )
    def test_load_state_success(
        self,
        mock_get_integration_account_id_from_env,
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

        mock_get_integration_account_id_from_env.return_value = integration_account_id
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
                ),
                "model.test_project.model_b": ModelNode(
                    checksum="def456",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_b.sql",
                    sql_path="models/model_b.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="new_checksum",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
                ),
                "model.test_project.model_b": ModelNode(
                    checksum="def456",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_b.sql",
                    sql_path="models/model_b.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
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
                "model.test_project.model_a": ModelNode(
                    checksum="abc123",
                    freshness=Freshness.CLEAN,
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    reason="Node not seen before",
                )
            },
            edges=[],
        )
        source_freshness = SourceFreshness(sources={})

        update_state(state, parsed_dag, source_freshness)

        assert state.state == {}
