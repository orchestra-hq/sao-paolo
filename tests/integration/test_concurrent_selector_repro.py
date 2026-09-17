import json
import os
import subprocess
from pathlib import Path

import pytest

_REPRO_PROJECT = (
    Path(__file__).resolve().parents[2] / "tutorial" / "concurrent-selector-repro"
)


def _postgres_ci_configured() -> bool:
    return bool(
        os.environ.get("PGHOST")
        and os.environ.get("PGDATABASE")
        and os.environ.get("CI") == "true"
    )


requires_postgres = pytest.mark.skipif(
    not _postgres_ci_configured(),
    reason="Set PGHOST and PGDATABASE and CI=true to run (CI provides these).",
)


def _csr_env(tmp_path: Path, schema: str) -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PGPORT", "5432")
    env.setdefault("PGUSER", "postgres")
    env.setdefault("PGPASSWORD", "postgres")
    # ORCHESTRA_API_KEY selects the Orchestra HTTP backend regardless of any file
    # setting, and CI's environment carries one; drop it so state lands in the temp file.
    env.pop("ORCHESTRA_API_KEY", None)
    env["ORCHESTRA_STATE_FILE"] = str(tmp_path / "dbt_state.json")
    (tmp_path / "dbt_state.json").write_text('{"state": {}}', encoding="utf-8")
    (tmp_path / "profiles.yml").write_text(
        f"""
concurrent_selector_repro:
  target: dev
  outputs:
    dev:
      type: postgres
      host: {env["PGHOST"]}
      port: {env["PGPORT"]}
      user: {env["PGUSER"]}
      password: {env["PGPASSWORD"]}
      dbname: {env["PGDATABASE"]}
      schema: {schema}
      threads: 1
""",
        encoding="utf-8",
    )
    env["DBT_PROFILES_DIR"] = str(tmp_path)
    return env


def _run(
    args: list[str], env: dict[str, str], label: str
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args, cwd=_REPRO_PROJECT, env=env, capture_output=True, text=True
    )
    print(f"\n\n=== {label} STDOUT:\n{result.stdout}")
    print(f"\n\n=== {label} STDERR:\n{result.stderr}")
    return result


def _sources_json_results() -> dict[str, dict]:
    """dbt's own `target/sources.json`, written by the `dbt source freshness`
    invocation _run just triggered -- keyed by unique_id so tests can check what
    dbt actually computed for a specific source, not just how many it collected."""
    raw = json.loads((_REPRO_PROJECT / "target" / "sources.json").read_text())
    return {result["unique_id"]: result for result in raw["results"]}


@requires_postgres
def test_scoping_only_checks_the_source_a_direct_model_selector_actually_uses(
    tmp_path: Path,
) -> None:
    """selector_x is the simplest possible selector -- {method: fqn, value: model_a},
    no ancestor/descendant operators. With scoping on, only its one source
    (raw.raw_events) should be checked; raw_unused.raw_page_views, which nothing
    here selects, should be skipped entirely."""
    env = _csr_env(tmp_path, "csr_scoping_test_x")
    assert _run(["orc", "dbt", "seed"], env, "seed").returncode == 0

    unscoped = _run(
        ["orc", "dbt", "build", "--selector", "selector_x"], env, "unscoped"
    )
    assert unscoped.returncode == 0
    assert "Collected 3 source(s) information." in unscoped.stdout

    env["ORCHESTRA_SCOPE_SOURCE_FRESHNESS_TO_SELECTION"] = "true"
    scoped = _run(["orc", "dbt", "build", "--selector", "selector_x"], env, "scoped")
    assert scoped.returncode == 0
    assert "Collected 1 source(s) information." in scoped.stdout


@requires_postgres
def test_scoping_with_a_descendant_expanding_selector_still_finds_one_source(
    tmp_path: Path,
) -> None:
    """selector_y is more complex than selector_x: {method: fqn, value: model_a,
    children: true} resolves to TWO models (model_a and its descendant model_b).
    Both feed off the same single source, so scoping must still collect exactly
    that one source -- not double-count it, and not still miss it because
    paths_to_run now has more than one entry."""
    env = _csr_env(tmp_path, "csr_scoping_test_y")
    assert _run(["orc", "dbt", "seed"], env, "seed").returncode == 0

    env["ORCHESTRA_SCOPE_SOURCE_FRESHNESS_TO_SELECTION"] = "true"
    scoped = _run(["orc", "dbt", "build", "--selector", "selector_y"], env, "scoped")
    assert scoped.returncode == 0
    assert "Collected 1 source(s) information." in scoped.stdout


@requires_postgres
def test_scoping_with_a_union_selector_picks_up_both_sources(
    tmp_path: Path,
) -> None:
    """selector_z is a union of two independent, unrelated criteria -- model_a
    and stg_unused_page_views -- rather than a bare fqn or an ancestor/descendant
    expansion of one node. It deliberately touches both sources any model in
    this project actually uses (raw and raw_unused), so scoping should collect
    both: the exclusion in the other tests isn't a hardcoded special case, it
    just follows whatever the selector resolves to."""
    env = _csr_env(tmp_path, "csr_scoping_test_z")
    assert _run(["orc", "dbt", "seed"], env, "seed").returncode == 0

    env["ORCHESTRA_SCOPE_SOURCE_FRESHNESS_TO_SELECTION"] = "true"
    scoped = _run(["orc", "dbt", "build", "--selector", "selector_z"], env, "scoped")
    assert scoped.returncode == 0
    assert "Collected 2 source(s) information." in scoped.stdout


@requires_postgres
def test_scoping_with_a_bare_build_still_excludes_a_source_no_model_uses(
    tmp_path: Path,
) -> None:
    """No --select/--selector at all resolves to every model in the project, so
    scoping is a no-op for the sources those models actually reference: raw
    (used by model_a, configured with a loaded_at_query and no `freshness:`
    block) and raw_unused (used by stg_unused_page_views) both still get
    checked. raw_orphan -- also loaded_at_query, no freshness block, same as
    raw -- but referenced by no model at all, is excluded regardless, because
    it was never an ancestor of anything to begin with.

    Also inspects dbt's own target/sources.json to confirm raw.raw_events isn't
    just "not erroring" -- dbt's native loaded_at_query execution actually pulled
    back the real max(event_at) from the seed (2025-01-03 09:15:00, the latest row
    in seeds/raw_events.csv), not a fallback/default value."""
    env = _csr_env(tmp_path, "csr_scoping_test_bare")
    assert _run(["orc", "dbt", "seed"], env, "seed").returncode == 0

    env["ORCHESTRA_SCOPE_SOURCE_FRESHNESS_TO_SELECTION"] = "true"
    scoped = _run(["orc", "dbt", "build"], env, "scoped bare build")
    assert scoped.returncode == 0
    assert "Collected 2 source(s) information." in scoped.stdout
    assert "raw_orphan" not in scoped.stdout
    assert (
        "Unable to calculate source freshness for "
        "source.concurrent_selector_repro.raw.raw_events"
    ) not in scoped.stdout

    sources = _sources_json_results()
    assert set(sources) == {
        "source.concurrent_selector_repro.raw.raw_events",
        "source.concurrent_selector_repro.raw_unused.raw_page_views",
    }
    raw_events = sources["source.concurrent_selector_repro.raw.raw_events"]
    assert raw_events["status"] == "pass"
    # Exact offset depends on the session/connection timezone, which this test
    # doesn't pin -- assert the wall-clock value dbt read, not its UTC offset.
    assert raw_events["max_loaded_at"].startswith("2025-01-03T09:15:00")
