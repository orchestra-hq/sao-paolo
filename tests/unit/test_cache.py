from datetime import datetime

from pytest_httpx import HTTPXMock

from src.orchestra_dbt.cache import load_state, save_state
from src.orchestra_dbt.models import StateApiModel, StateItem


class TestLoadState:
    def test_load_state_success(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            url="https://dev.getorchestra.io/api/engine/public/state/test-cache-key",
            match_headers={
                "Accept": "application/json",
                "Authorization": "Bearer test-api-key",
            },
            json={
                "state": {
                    "model.test": {
                        "last_updated": "2024-01-01T12:00:00",
                        "checksum": "123",
                    }
                }
            },
        )
        assert load_state() == StateApiModel(
            state={
                "model.test": StateItem(
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="123",
                )
            }
        )

    def test_load_state_http_error(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            url="https://dev.getorchestra.io/api/engine/public/state/test-cache-key",
            match_headers={
                "Accept": "application/json",
                "Authorization": "Bearer test-api-key",
            },
            status_code=400,
        )
        assert load_state() == StateApiModel(state={})

    def test_load_state_validation_error(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            url="https://dev.getorchestra.io/api/engine/public/state/test-cache-key",
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
            url="https://dev.getorchestra.io/api/engine/public/state/test-cache-key",
            match_headers={
                "Authorization": "Bearer test-api-key",
                "Content-Type": "application/json",
            },
            match_json={
                "state": {
                    "model.test": {
                        "last_updated": "2024-01-01T14:00:00",
                        "checksum": "123",
                    },
                    "model.new": {
                        "last_updated": "2024-01-01T14:00:00",
                        "checksum": "456",
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
                        ),
                        "model.new": StateItem(
                            last_updated=datetime(2024, 1, 1, 14, 0, 0),
                            checksum="456",
                        ),
                    }
                )
            )
            is None
        )

    def test_save_state_http_error(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="PATCH",
            url="https://dev.getorchestra.io/api/engine/public/state/test-cache-key",
            match_headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer test-api-key",
            },
            status_code=500,
        )
        assert save_state(state=StateApiModel(state={})) is None
