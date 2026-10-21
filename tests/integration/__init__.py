import os
from pathlib import Path

import pytest

# The repro projects share the tutorial's profile: postgres by default, duckdb with
# DBT_TARGET=duckdb.
TUTORIAL_PROFILES_DIR = Path(__file__).resolve().parents[2] / "tutorial" / "dbt"


def _postgres_ci_configured() -> bool:
    return bool(
        os.environ.get("PGHOST")
        and os.environ.get("PGDATABASE")
        and os.environ.get("CI") == "true"
    )


# The duckdb target needs no service, so it also covers dbt 2.x, which rejects postgres.
requires_warehouse = pytest.mark.skipif(
    not (_postgres_ci_configured() or os.environ.get("DBT_TARGET") == "duckdb"),
    reason="Set PGHOST, PGDATABASE and CI=true, or DBT_TARGET=duckdb, to run.",
)
