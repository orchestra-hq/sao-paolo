"""Real dbt (no live warehouse needed -- `dbt ls` only parses/selects) proving
scope_source_freshness_to_selection works when the triggering command used a
named `--selector` whose own definition is just a bare model name, with no
ancestor/descendant operators at all -- the simplest selector dbt allows.
"""

import os
from pathlib import Path

import pytest

from src.orchestra_dbt.ls import get_nodes_to_run
from src.orchestra_dbt.source_freshness import get_args_for_source_freshness
from tests.integration import TUTORIAL_PROFILES_DIR, requires_warehouse

_REPRO_PROJECT = (
    Path(__file__).resolve().parents[2] / "tutorial" / "concurrent-selector-repro"
)


@requires_warehouse
def test_bare_model_name_selector_resolves_and_scopes_to_its_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # selector_x in this project's selectors.yml: {method: fqn, value: model_a}.
    monkeypatch.chdir(_REPRO_PROJECT)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(TUTORIAL_PROFILES_DIR))

    nodes = get_nodes_to_run(("--selector", "selector_x"), _REPRO_PROJECT / "target")

    assert nodes is not None
    assert nodes.paths == ["models/model_a.sql"]
    assert nodes.selectors == ["concurrent_selector_repro.model_a"]
    assert get_args_for_source_freshness(
        (), scope_to_selection=True, selectors_to_run=nodes.selectors
    ) == [
        "source",
        "freshness",
        "-q",
        # Forwarded from the environment, which picks the tutorial profile's target.
        *(["--target", os.environ["DBT_TARGET"]] if "DBT_TARGET" in os.environ else []),
        "--select",
        "+concurrent_selector_repro.model_a",
    ]
