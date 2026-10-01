from datetime import UTC, datetime

import httpx
import pytest
from pytest_httpx import HTTPXMock

from src.orchestra_dbt.models import StateApiModel, StateItem
from src.orchestra_dbt.state_backends.http import HttpStateBackend

STATE_URL = "https://dev.getorchestra.io/api/engine/public/state/DBT_CORE"

STATE = StateApiModel(
    state={
        "model.test": StateItem(
            last_updated=datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC),
            checksum="123",
            sources={},
        )
    }
)


@pytest.fixture(autouse=True)
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    recorded: list[float] = []
    monkeypatch.setattr("time.sleep", recorded.append)
    return recorded


class TestHttpStateBackendSave:
    def test_saves_without_retrying(self, httpx_mock: HTTPXMock, sleeps):
        httpx_mock.add_response(method="PATCH", url=STATE_URL)

        HttpStateBackend().save(STATE)

        assert len(httpx_mock.get_requests()) == 1
        assert sleeps == []

    @pytest.mark.parametrize("status_code", [429, 500, 503])
    def test_retries_transient_status_then_saves(
        self, httpx_mock: HTTPXMock, sleeps, status_code: int
    ):
        httpx_mock.add_response(method="PATCH", url=STATE_URL, status_code=status_code)
        httpx_mock.add_response(method="PATCH", url=STATE_URL)

        HttpStateBackend().save(STATE)

        assert len(httpx_mock.get_requests()) == 2
        assert len(sleeps) == 1

    def test_retries_network_error_then_saves(self, httpx_mock: HTTPXMock, sleeps):
        httpx_mock.add_exception(
            httpx.ReadTimeout("timed out"), method="PATCH", url=STATE_URL
        )
        httpx_mock.add_response(method="PATCH", url=STATE_URL)

        HttpStateBackend().save(STATE)

        assert len(httpx_mock.get_requests()) == 2
        assert len(sleeps) == 1

    def test_warns_after_three_failed_attempts(
        self, httpx_mock: HTTPXMock, sleeps, capsys: pytest.CaptureFixture[str]
    ):
        for _ in range(3):
            httpx_mock.add_response(method="PATCH", url=STATE_URL, status_code=500)

        HttpStateBackend().save(STATE)

        assert len(httpx_mock.get_requests()) == 3
        assert len(sleeps) == 2
        assert "Failed to save state (500)" in capsys.readouterr().out

    def test_does_not_retry_client_errors(
        self, httpx_mock: HTTPXMock, sleeps, capsys: pytest.CaptureFixture[str]
    ):
        httpx_mock.add_response(method="PATCH", url=STATE_URL, status_code=403)

        HttpStateBackend().save(STATE)

        assert len(httpx_mock.get_requests()) == 1
        assert sleeps == []
        assert "Failed to save state (403)" in capsys.readouterr().out
