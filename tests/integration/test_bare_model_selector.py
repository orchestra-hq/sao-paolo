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
from tests.integration import parse_only_output

_REPRO_PROJECT = (
    Path(__file__).resolve().parents[2] / "tutorial" / "concurrent-selector-repro"
)


def test_bare_model_name_selector_resolves_and_scopes_to_its_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = parse_only_output()
    if output is None:
        pytest.skip("needs any one dbt adapter installed (to register, not connect)")

    # selector_x in this project's selectors.yml: {method: fqn, value: model_a}.
    # No live connection needed -- dbt ls only has to render the profile.
    # dbt reads DBT_TARGET too, so the output is named after it.
    target = os.environ.get("DBT_TARGET", "dev")
    (tmp_path / "profiles.yml").write_text(
        f"""
concurrent_selector_repro:
  target: {target}
  outputs:
    {target}:{output}      schema: csr_selector_test
      threads: 1
"""
    )
    monkeypatch.chdir(_REPRO_PROJECT)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path))

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
        *(["--target", target] if "DBT_TARGET" in os.environ else []),
        "--select",
        "+concurrent_selector_repro.model_a",
    ]
