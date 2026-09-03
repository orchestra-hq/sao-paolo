import json
import os
import subprocess
from pathlib import Path

import pytest

_TUTORIAL_DBT = Path(__file__).resolve().parents[2] / "tutorial" / "dbt"


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


def _tutorial_env(state_file: Path | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PGPORT", "5432")
    env.setdefault("PGUSER", "postgres")
    env.setdefault("PGPASSWORD", "postgres")
    env.setdefault("DBT_SCHEMA", "sao_tutorial")
    env["DBT_PROFILES_DIR"] = str(_TUTORIAL_DBT)
    if state_file is not None:
        # ORCHESTRA_API_KEY selects the Orchestra HTTP backend regardless of any file
        # setting, and CI's environment carries one. Drop it so state really lands in the
        # temp file: these tests are about reuse, and reuse needs state that loads.
        env.pop("ORCHESTRA_API_KEY", None)
        env["ORCHESTRA_STATE_FILE"] = str(state_file)
        # Every model in this project descends from the `raw_events` seed, and seeds are
        # unconditionally dirty unless this is on -- which would make *nothing* reusable
        # and leave these tests unable to reach the state they are asserting about.
        env["ORCHESTRA_SEED_STATE_ORCHESTRATION"] = "true"
        # Off by default -- these tests are what proves it works, so opt in.
        env["ORCHESTRA_VERIFY_RELATIONS_EXIST"] = "true"
    return env


def _run_build(env: dict[str, str], label: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["orc", "dbt", "build"],
        cwd=_TUTORIAL_DBT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    print(f"\n\n=== {label} STDOUT:\n{result.stdout}")
    print(f"\n\n=== {label} STDERR:\n{result.stderr}")
    return result


def _connect(env: dict[str, str]):
    psycopg2 = pytest.importorskip(
        "psycopg2", reason="Needs the dbt-postgres adapter (the `adapters` extra)."
    )
    return psycopg2.connect(
        host=env["PGHOST"],
        port=env["PGPORT"],
        user=env["PGUSER"],
        password=env["PGPASSWORD"],
        dbname=env["PGDATABASE"],
    )


def _relation_exists(env: dict[str, str], schema: str, identifier: str) -> bool:
    with _connect(env) as connection, connection.cursor() as cursor:
        cursor.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = %s and table_name = %s",
            (schema, identifier),
        )
        return cursor.fetchone()[0] > 0


def _drop_view(env: dict[str, str], schema: str, identifier: str) -> None:
    """Drop a relation behind dbt's back. CASCADE also takes out downstream views."""
    with _connect(env) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f'drop view if exists "{schema}"."{identifier}" cascade')


def _assert_state_loaded(result: subprocess.CompletedProcess[str]) -> None:
    """Reuse is impossible without state, so surface that as the real cause."""
    assert "Could not load state" not in result.stdout, (
        "state backend did not load; the run cannot reuse anything"
    )


@requires_postgres
def test_tutorial_dbt_build_succeeds() -> None:
    result = _run_build(_tutorial_env(), "build")

    if result.returncode != 0:
        pytest.fail(
            f"dbt build failed ({result.returncode})\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


@requires_postgres
def test_dropped_relation_is_rebuilt_instead_of_reused(tmp_path: Path) -> None:
    """A model whose relation was deleted must be rerun, not skipped."""
    state_file = tmp_path / "dbt_state.json"
    state_file.write_text(json.dumps({"state": {}}), encoding="utf-8")
    env = _tutorial_env(state_file)
    staging_schema = f"{env['DBT_SCHEMA']}_staging"
    intermediate_schema = f"{env['DBT_SCHEMA']}_intermediate"

    assert _run_build(env, "seed run").returncode == 0

    warm = _run_build(env, "warm run")
    assert warm.returncode == 0
    _assert_state_loaded(warm)
    assert "REUSED model.sao_tutorial.stg_events" in warm.stdout, (
        "stg_events should be reused before we drop it, or this test proves nothing"
    )

    _drop_view(env, staging_schema, "stg_events")
    assert not _relation_exists(env, staging_schema, "stg_events")
    assert not _relation_exists(env, intermediate_schema, "int_events_enriched")

    repaired = _run_build(env, "repair run")
    assert repaired.returncode == 0
    assert "REUSED model.sao_tutorial.stg_events" not in repaired.stdout
    assert "deleted from the warehouse hence rerun" in repaired.stdout
    assert _relation_exists(env, staging_schema, "stg_events")
    assert _relation_exists(env, intermediate_schema, "int_events_enriched")


@requires_postgres
def test_disabling_the_check_reproduces_the_silent_skip(tmp_path: Path) -> None:
    """Negative control: with the check off, a dropped relation is silently left missing."""
    state_file = tmp_path / "dbt_state.json"
    state_file.write_text(json.dumps({"state": {}}), encoding="utf-8")
    env = _tutorial_env(state_file)
    staging_schema = f"{env['DBT_SCHEMA']}_staging"

    assert _run_build(env, "seed run").returncode == 0
    warm = _run_build(env, "warm run")
    assert warm.returncode == 0
    _assert_state_loaded(warm)
    assert "REUSED model.sao_tutorial.stg_events" in warm.stdout

    _drop_view(env, staging_schema, "stg_events")

    env["ORCHESTRA_VERIFY_RELATIONS_EXIST"] = "false"  # i.e. the default
    skipped = _run_build(env, "check disabled")

    # The run "succeeds" while the relation stays missing.
    assert skipped.returncode == 0
    assert "REUSED model.sao_tutorial.stg_events" in skipped.stdout
    assert not _relation_exists(env, staging_schema, "stg_events")

    # Leave the warehouse usable for anything that runs after this.
    env["ORCHESTRA_VERIFY_RELATIONS_EXIST"] = "true"
    assert _run_build(env, "restore").returncode == 0
    assert _relation_exists(env, staging_schema, "stg_events")
