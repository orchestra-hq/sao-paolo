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
        env["ORCHESTRA_STATE_FILE"] = str(state_file)
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
    """Drop a relation behind dbt's back, the way a person or another job would.

    CASCADE is required: `int_events_enriched` is a view on top of `stg_events`, so Postgres
    refuses a plain drop. That means the drop takes out two relations, which is useful -- it
    exercises both the direct check and downstream propagation.
    """
    with _connect(env) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f'drop view if exists "{schema}"."{identifier}" cascade')


@requires_postgres
def test_tutorial_dbt_build_succeeds() -> None:
    result = _run_build(_tutorial_env(), "build")

    if result.returncode != 0:
        pytest.fail(
            f"dbt build failed ({result.returncode})\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


@requires_postgres
def test_dropped_relation_is_rebuilt_instead_of_reused(tmp_path: Path) -> None:
    """A model whose relation was deleted must be rerun, not skipped.

    Without the existence check the second build reuses `stg_events` from state and never
    notices the view is gone.
    """
    state_file = tmp_path / "dbt_state.json"
    state_file.write_text(json.dumps({"state": {}}), encoding="utf-8")
    env = _tutorial_env(state_file)
    staging_schema = f"{env['DBT_SCHEMA']}_staging"
    intermediate_schema = f"{env['DBT_SCHEMA']}_intermediate"

    # 1. Populate state.
    assert _run_build(env, "seed run").returncode == 0

    # 2. With warm state and no changes, dbt-orchestra should be reusing stg_events.
    warm = _run_build(env, "warm run")
    assert warm.returncode == 0
    assert "REUSED model.sao_tutorial.stg_events" in warm.stdout, (
        "expected stg_events to be reused before we drop it; "
        "state-aware reuse is not engaging so this test proves nothing"
    )

    # 3. Delete the relation out of band.
    _drop_view(env, staging_schema, "stg_events")
    assert not _relation_exists(env, staging_schema, "stg_events")
    assert not _relation_exists(env, intermediate_schema, "int_events_enriched")

    # 4. The next build must rerun it rather than skip it.
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
    assert "REUSED model.sao_tutorial.stg_events" in warm.stdout

    _drop_view(env, staging_schema, "stg_events")

    env["ORCHESTRA_VERIFY_RELATIONS_EXIST"] = "false"
    skipped = _run_build(env, "check disabled")

    # The run "succeeds" while the relation stays missing -- the bug the check exists to fix.
    assert skipped.returncode == 0
    assert "REUSED model.sao_tutorial.stg_events" in skipped.stdout
    assert not _relation_exists(env, staging_schema, "stg_events")

    # Leave the warehouse usable for anything that runs after this.
    del env["ORCHESTRA_VERIFY_RELATIONS_EXIST"]
    assert _run_build(env, "restore").returncode == 0
    assert _relation_exists(env, staging_schema, "stg_events")
