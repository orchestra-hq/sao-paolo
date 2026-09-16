"""Real dbt (no live warehouse needed -- `dbt deps`/`dbt ls` only parse and
select) against a project whose models are ALL owned by an installed package.

This is the shape that silently broke source-freshness scoping: `dbt ls` reports
paths relative to the owning package, but dbt resolves `path:` criteria by
globbing the real filesystem from the project root, where those files don't
exist. See tutorial/package-only-repro/README.md.
"""

import importlib.util
from pathlib import Path

import pytest

from src.orchestra_dbt.ls import get_nodes_to_run
from src.orchestra_dbt.source_freshness import get_args_for_source_freshness

_REPRO_PROJECT = Path(__file__).resolve().parents[2] / "tutorial" / "package-only-repro"

_MODEL_PATH = "models/staging/stg_orders.sql"
_MODEL_FQN = "analytics_pkg.staging.stg_orders"
_SOURCE = "raw.raw_orders"

# Nothing here connects -- dbt only has to *register* an adapter to render the
# profile and parse. Any installed adapter will do, so take whichever this
# environment has (CI's test job has none and skips; tutorial-dbt has postgres).
_ADAPTER_OUTPUTS = {
    "postgres": """
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: postgres
""",
    "snowflake": """
      type: snowflake
      account: fake_account
      user: fake_user
      password: fake_password
      role: fake_role
      database: fake_db
      warehouse: fake_wh
""",
    "databricks": """
      type: databricks
      host: fake.cloud.databricks.com
      http_path: /sql/1.0/warehouses/fake
      token: fake_token
      catalog: fake_catalog
""",
}


@pytest.fixture
def package_only_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Install the vendored package and point dbt at a profile it can render."""
    output = next(
        (
            block
            for name, block in _ADAPTER_OUTPUTS.items()
            if importlib.util.find_spec(f"dbt.adapters.{name}")
        ),
        None,
    )
    if output is None:
        pytest.skip("needs any one dbt adapter installed (to register, not connect)")

    from dbt.cli.main import dbtRunner

    (tmp_path / "profiles.yml").write_text(
        f"""
package_only_root:
  target: dev
  outputs:
    dev:{output}      schema: package_only_repro
      threads: 1
"""
    )
    monkeypatch.chdir(_REPRO_PROJECT)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path))

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
    nodes = get_nodes_to_run(())

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
    nodes = get_nodes_to_run(())
    assert nodes is not None

    args = get_args_for_source_freshness(
        (), scope_to_selection=True, selectors_to_run=nodes.selectors
    )

    assert args == ["source", "freshness", "-q", "--select", f"+{_MODEL_FQN}"]

    criteria = args[args.index("--select") + 1]
    assert _sources_selected_by(package_only_project, criteria) == [_SOURCE]
