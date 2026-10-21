"""Real dbt (no live warehouse needed -- `dbt deps`/`dbt ls` only parse and
select) against a project whose models are ALL owned by an installed package.

This is the shape that silently broke source-freshness scoping: `dbt ls` reports
paths relative to the owning package, but dbt resolves `path:` criteria by
globbing the real filesystem from the project root, where those files don't
exist. See tutorial/package-only-repro/README.md.
"""

import os
from pathlib import Path

import pytest

from src.orchestra_dbt.compatibility import is_dbt_v2
from src.orchestra_dbt.ls import get_args_for_ls, get_nodes_to_run
from src.orchestra_dbt.source_freshness import get_args_for_source_freshness
from tests.integration import TUTORIAL_PROFILES_DIR, requires_warehouse

_REPRO_PROJECT = Path(__file__).resolve().parents[2] / "tutorial" / "package-only-repro"

pytestmark = requires_warehouse

_MODEL_PATH = "models/staging/stg_orders.sql"
_MODEL_FQN = "analytics_pkg.staging.stg_orders"
# dbt 2.x's ls ignores `--output name` and prints a selector instead.
_SOURCE = "source:analytics_pkg.raw.raw_orders" if is_dbt_v2() else "raw.raw_orders"


@pytest.fixture
def package_only_project(monkeypatch: pytest.MonkeyPatch):
    """Install the vendored package and point dbt at a profile it can render."""
    from dbt.cli.main import dbtRunner

    monkeypatch.chdir(_REPRO_PROJECT)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(TUTORIAL_PROFILES_DIR))

    deps = dbtRunner().invoke(["deps", "-q"])
    assert deps.success, f"dbt deps failed: {deps.exception}"
    return dbtRunner


def _sources_selected_by(dbt_runner, criteria: str) -> list[str]:
    """Which sources does dbt actually resolve this --select down to?"""
    result = dbt_runner().invoke(
        [
            "ls",
            "--resource-type",
            "source",
            "--select",
            criteria,
            "--output",
            "name",
            "-q",
        ]
    )
    assert result.success, f"dbt ls failed: {result.exception}"
    return list(result.result)


def test_ls_reports_package_relative_paths_that_do_not_exist_at_the_root(
    package_only_project,
) -> None:
    """The premise of the bug: the reported path is real to dbt but is not a real
    file relative to the project root, because the package owns it."""
    nodes = get_nodes_to_run((), _REPRO_PROJECT / "target")

    assert nodes is not None
    assert nodes.paths == [_MODEL_PATH]
    assert nodes.selectors == [_MODEL_FQN]

    assert not (_REPRO_PROJECT / _MODEL_PATH).exists()
    assert (_REPRO_PROJECT / "dbt_packages" / "analytics_pkg" / _MODEL_PATH).exists()


def test_path_selection_finds_nothing_but_fqn_selection_finds_the_source(
    package_only_project,
) -> None:
    """The fix, proven end to end against real dbt selection.

    `+path:` resolves to zero sources here -- that is the bug, and it is why the
    freshness run came back empty with no error at all. `+<fqn>` resolves to the
    source the model actually depends on.
    """
    assert _sources_selected_by(package_only_project, f"+path:{_MODEL_PATH}") == []
    assert _sources_selected_by(package_only_project, f"+{_MODEL_FQN}") == [_SOURCE]


def test_scoped_freshness_args_resolve_to_the_source(package_only_project) -> None:
    """End to end: what get_args_for_source_freshness builds for this project has to
    actually select the source when handed back to dbt."""
    nodes = get_nodes_to_run((), _REPRO_PROJECT / "target")
    assert nodes is not None

    args = get_args_for_source_freshness(
        (), scope_to_selection=True, selectors_to_run=nodes.selectors
    )

    # The target is forwarded from the environment, which picks the profile's target.
    target = (
        ["--target", os.environ["DBT_TARGET"]] if "DBT_TARGET" in os.environ else []
    )
    assert args == ["source", "freshness", "-q", *target, "--select", f"+{_MODEL_FQN}"]

    criteria = args[args.index("--select") + 1]
    assert _sources_selected_by(package_only_project, criteria) == [_SOURCE]


@pytest.mark.skipif(
    is_dbt_v2(), reason="dbt 2.x's ls ignores --output, so there's no path output"
)
def test_json_paths_are_identical_to_the_previous_path_output(
    package_only_project,
) -> None:
    """Switching `dbt ls` to JSON must not change the paths themselves.

    They are compared against each node's `dbt_path`, which construct_dag reads
    from the manifest's `original_file_path` -- the same field `--output path`
    emitted. Pin that here, on a project where the paths are package-relative and
    so least likely to match by accident.
    """
    # Derived, not hardcoded: if RESOURCE_TYPES_TO_LS ever changes, both sides must
    # still list the same nodes or this pins nothing.
    legacy_args = get_args_for_ls(())
    legacy_args[legacy_args.index("--output") :] = ["--output", "path", "-q"]
    legacy = package_only_project().invoke(legacy_args)
    assert legacy.success, f"dbt ls failed: {legacy.exception}"

    nodes = get_nodes_to_run((), _REPRO_PROJECT / "target")

    assert nodes is not None
    assert nodes.paths == list(legacy.result)
