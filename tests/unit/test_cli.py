"""Covers `cli.py` wiring the unit suite otherwise never exercises.

Both regressions guarded here silently disable the feature while every other test passes,
and the integration tests that would catch them are CI/Postgres-gated.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from src.orchestra_dbt import cli
from src.orchestra_dbt.config import OrchestraDbtSettings
from src.orchestra_dbt.models import ParsedDag, SourceFreshness, StateApiModel


@pytest.fixture
def stub_run(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Stub everything around the gate, recording the order of the two calls that matter."""
    calls: list[str] = []

    monkeypatch.setattr(
        cli,
        "load_orchestra_dbt_settings",
        lambda _project_dir: OrchestraDbtSettings(
            use_stateful=True, verify_relations_exist=True
        ),
    )
    monkeypatch.setattr(cli, "_validate_environment", lambda _settings: None)
    monkeypatch.setattr(cli, "get_nodes_to_run", lambda *a, **k: None)
    # Tolerate added kwargs: this fixture must not break when a caller gains an option.
    monkeypatch.setattr(
        cli, "get_source_freshness", lambda **k: SourceFreshness(sources={})
    )
    monkeypatch.setattr(cli, "load_state", lambda *a, **k: StateApiModel(state={}))
    monkeypatch.setattr(
        cli, "find_packages_install_path", lambda *a: Path("dbt_packages")
    )
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


def _run(*args: str) -> None:
    """Invoke the CLI, surfacing anything CliRunner would otherwise swallow."""
    result = CliRunner().invoke(cli.main, list(args) or ["dbt", "build"])
    if result.exception is not None and not isinstance(result.exception, SystemExit):
        raise AssertionError(
            f"cli.main blew up: {result.exception!r}"
        ) from result.exception


def test_gate_runs_before_the_dependency_sweep(stub_run: list[str]) -> None:
    """Order matters: after the sweep, a forced-dirty node never propagates downstream."""
    _run()

    assert stub_run == ["gate", "sweep"]


def test_stateful_run_when_a_global_flag_precedes_the_subcommand(
    stub_run: list[str],
) -> None:
    _run("dbt", "--target", "prod", "build")

    assert stub_run == ["gate", "sweep"]


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (("dbt", "build", "-s", "x"), ("dbt", "build", "-s", "x")),
        (("dbt", "--debug", "build"), ("dbt", "build", "--debug")),
        (
            ("dbt", "--target", "prod", "build", "-s", "x"),
            ("dbt", "build", "--target", "prod", "-s", "x"),
        ),
        (("dbt", "--target=prod", "run"), ("dbt", "run", "--target=prod")),
        (("dbt", "-t", "prod", "test"), ("dbt", "test", "-t", "prod")),
        (
            ("dbt", "--log-level", "debug", "build"),
            ("dbt", "build", "--log-level", "debug"),
        ),
        (("dbt", "--target", "prod"), ("dbt", "--target", "prod")),
    ],
)
def test_subcommand_first(args: tuple[str, ...], expected: tuple[str, ...]) -> None:
    assert cli._subcommand_first(args) == expected


def test_gate_is_skipped_when_the_setting_is_disabled(
    stub_run: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        cli,
        "load_orchestra_dbt_settings",
        lambda _project_dir: OrchestraDbtSettings(
            use_stateful=True, verify_relations_exist=False
        ),
    )

    _run()

    assert stub_run == ["sweep"]


def test_warns_when_scoping_is_on_but_the_selection_could_not_be_resolved(
    stub_run: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed `dbt ls` leaves freshness unscoped, checking every source. Correct,
    but it ignores the setting, so it must not happen silently -- and unlike an empty
    result it isn't visible from the `Collected N source(s)` count."""
    warnings: list[str] = []
    monkeypatch.setattr(
        cli,
        "load_orchestra_dbt_settings",
        lambda _project_dir: OrchestraDbtSettings(
            use_stateful=True, scope_source_freshness_to_selection=True
        ),
    )
    monkeypatch.setattr(cli, "log_warn", lambda msg: warnings.append(msg))

    _run()

    assert any("Could not resolve the selection" in w for w in warnings)


def test_stays_quiet_when_scoping_is_off(
    stub_run: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(cli, "log_warn", lambda msg: warnings.append(msg))

    _run()

    assert not any("Could not resolve the selection" in w for w in warnings)
