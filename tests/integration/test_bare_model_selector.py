"""Real dbt (no live warehouse needed -- `dbt ls` only parses/selects) proving
scope_source_freshness_to_selection works when the triggering command used a
named `--selector` whose own definition is just a bare model name, with no
ancestor/descendant operators at all -- the simplest selector dbt allows.
"""

from pathlib import Path

import pytest

from src.orchestra_dbt.ls import get_nodes_to_run
from src.orchestra_dbt.source_freshness import get_args_for_source_freshness

_REPRO_PROJECT = (
    Path(__file__).resolve().parents[2] / "tutorial" / "concurrent-selector-repro"
)


def test_bare_model_name_selector_resolves_and_scopes_to_its_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("dbt.adapters.postgres")

    # selector_x in this project's selectors.yml: {method: fqn, value: model_a}.
    # No live connection needed -- dbt ls only has to render the profile.
    (tmp_path / "profiles.yml").write_text(
        """
concurrent_selector_repro:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: postgres
      schema: csr_selector_test
      threads: 1
"""
    )
    monkeypatch.chdir(_REPRO_PROJECT)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path))

    nodes = get_nodes_to_run(("--selector", "selector_x"))

    assert nodes is not None
    assert nodes.paths == ["models/model_a.sql"]
    assert nodes.selectors == ["concurrent_selector_repro.model_a"]
    assert get_args_for_source_freshness(
        (), scope_to_selection=True, selectors_to_run=nodes.selectors
    ) == [
        "source",
        "freshness",
        "-q",
        "--select",
        "+concurrent_selector_repro.model_a",
    ]


def test_no_select_or_selector_resolves_to_every_model_in_the_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bare `dbt build`, with no `--select`/`--selector` at all, resolves to
    every node in the project. Scoping source freshness to that is then a
    no-op, covering every source, same as unscoped.

    Uses the same dummy postgres profile as the sibling test above -- dbt ls
    only needs a profile it can render, not a live connection -- since that's
    the only adapter CI's `tutorial-dbt` job (the one that runs this file, per
    ci.yaml) installs.
    """
    pytest.importorskip("dbt.adapters.postgres")

    (tmp_path / "profiles.yml").write_text(
        """
concurrent_selector_repro:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: postgres
      schema: csr_bare_test
      threads: 1
"""
    )
    monkeypatch.chdir(_REPRO_PROJECT)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path))

    nodes = get_nodes_to_run(())

    assert nodes is not None
    assert sorted(nodes.paths) == [
        "models/model_a.sql",
        "models/model_b.sql",
        "models/stg_unused_page_views.sql",
        "seeds/raw_events.csv",
        "seeds/raw_page_views.csv",
    ]
    selectors = sorted(nodes.selectors)
    assert selectors == [
        "concurrent_selector_repro.model_a",
        "concurrent_selector_repro.model_b",
        "concurrent_selector_repro.raw_events",
        "concurrent_selector_repro.raw_page_views",
        "concurrent_selector_repro.stg_unused_page_views",
    ]
    assert get_args_for_source_freshness(
        (), scope_to_selection=True, selectors_to_run=selectors
    ) == [
        "source",
        "freshness",
        "-q",
        "--select",
        *(f"+{selector}" for selector in selectors),
    ]
