import os
import subprocess
from pathlib import Path

import pytest

_TUTORIAL_DBT = Path(__file__).resolve().parents[2] / "tutorial" / "dbt"


def _postgres_configured() -> bool:
    return bool(os.environ.get("PGHOST") and os.environ.get("PGDATABASE"))


@pytest.mark.skipif(
    not _postgres_configured(),
    reason="Set PGHOST and PGDATABASE to run (CI provides these).",
)
def test_tutorial_dbt_build_succeeds() -> None:
    env = os.environ.copy()
    env.setdefault("PGPORT", "5432")
    env.setdefault("PGUSER", "postgres")
    env.setdefault("PGPASSWORD", "postgres")
    env.setdefault("DBT_SCHEMA", "sao_tutorial")
    env["DBT_PROFILES_DIR"] = str(_TUTORIAL_DBT)

    result = subprocess.run(
        ["dbt", "build", "--target", "ci"],
        cwd=_TUTORIAL_DBT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"dbt build failed ({result.returncode})\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
