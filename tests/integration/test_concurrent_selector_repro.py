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


def _csr_env(tmp_path: Path) -> dict[str, str]:
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
      schema: csr_source_scoping_test
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


@requires_postgres
def test_scoping_only_checks_the_source_a_direct_model_selector_actually_uses(
    tmp_path: Path,
) -> None:
    """selector_x is the simplest possible selector -- {method: fqn, value: model_a},
    no ancestor/descendant operators. With scoping on, only its one source
    (raw.raw_events) should be checked; raw_unused.raw_page_views, which nothing
    here selects, should be skipped entirely."""
    env = _csr_env(tmp_path)
    assert _run(["orc", "dbt", "seed"], env, "seed").returncode == 0

    unscoped = _run(
        ["orc", "dbt", "build", "--selector", "selector_x"], env, "unscoped"
    )
    assert unscoped.returncode == 0
    assert "Collected 2 source(s) information." in unscoped.stdout

    env["ORCHESTRA_SCOPE_SOURCE_FRESHNESS_TO_SELECTION"] = "true"
    scoped = _run(["orc", "dbt", "build", "--selector", "selector_x"], env, "scoped")
    assert scoped.returncode == 0
    assert "Collected 1 source(s) information." in scoped.stdout
