"""Real dbt (no live warehouse needed -- `dbt ls` only parses/selects) proving
scope_source_freshness_to_selection works when the triggering command used a
named `--selector` whose own definition is just a bare model name, with no
ancestor/descendant operators at all -- the simplest selector dbt allows.
"""

from pathlib import Path

import pytest

from src.orchestra_dbt.ls import get_paths_to_run
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

    paths = get_paths_to_run(("--selector", "selector_x"))

    assert paths == ["models/model_a.sql"]
    assert get_args_for_source_freshness(
        (), scope_to_selection=True, paths_to_run=paths
    ) == [
        "source",
        "freshness",
        "-q",
        "--select",
        "+path:models/model_a.sql",
    ]
