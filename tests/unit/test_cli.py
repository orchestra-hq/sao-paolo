"""Covers `cli.py` wiring the unit suite otherwise never exercises.

Both regressions guarded here silently disable the feature while every other test passes,
and the integration tests that would catch them are CI/Postgres-gated.
"""

from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

import src.orchestra_dbt.cli as cli
from src.orchestra_dbt.config import OrchestraDbtSettings
from src.orchestra_dbt.models import ParsedDag, SourceFreshness, StateApiModel


@pytest.fixture
def stub_run(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Stub everything around the gate, recording the order of the two calls that matter."""
    calls: list[str] = []

    monkeypatch.setattr(
        cli,
        "load_orchestra_dbt_settings",
        lambda: OrchestraDbtSettings(use_stateful=True, verify_relations_exist=True),
    )
    monkeypatch.setattr(cli, "_validate_environment", lambda: None)
    monkeypatch.setattr(cli, "get_paths_to_run", lambda *a, **k: None)
    # Tolerate added kwargs: this fixture must not break when a caller gains an option.
    monkeypatch.setattr(
        cli, "get_source_freshness", lambda **k: SourceFreshness(sources={})
    )
    monkeypatch.setattr(cli, "load_state", lambda *a, **k: StateApiModel(state={}))
    monkeypatch.setattr(
        cli, "construct_dag", lambda *a, **k: ParsedDag(nodes={}, edges=[])
    )
    monkeypatch.setattr(cli, "propagate_freshness_config", lambda *a, **k: None)
    monkeypatch.setattr(cli, "is_full_refresh_requested", lambda *a, **k: False)
    monkeypatch.setattr(cli, "subprocess", MagicMock())
    monkeypatch.setattr(cli, "_complete_run", MagicMock(side_effect=SystemExit(0)))

    monkeypatch.setattr(
        cli,
        "apply_relation_existence_gate",
        lambda *a, **k: calls.append("gate"),
    )
    monkeypatch.setattr(
        cli, "calculate_nodes_to_run", lambda *a, **k: calls.append("sweep")
    )
    return calls


def _run() -> None:
    """Invoke the CLI, surfacing anything CliRunner would otherwise swallow."""
    result = CliRunner().invoke(cli.main, ["dbt", "build"])
    if result.exception is not None and not isinstance(result.exception, SystemExit):
        raise AssertionError(
            f"cli.main blew up: {result.exception!r}"
        ) from result.exception


def test_gate_runs_before_the_dependency_sweep(stub_run: list[str]) -> None:
    """Order matters: after the sweep, a forced-dirty node never propagates downstream."""
    _run()

    assert stub_run == ["gate", "sweep"]


def test_gate_is_skipped_when_the_setting_is_disabled(
    stub_run: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        cli,
        "load_orchestra_dbt_settings",
        lambda: OrchestraDbtSettings(use_stateful=True, verify_relations_exist=False),
    )

    _run()

    assert stub_run == ["sweep"]
