"""
Smoke tests against the installed dbt-core: internal import paths used by orchestra-dbt.

These fail early when dbt-core breaks compatibility (e.g. moved modules), without needing a warehouse.

Imports are inside each test so collection does not load dbt unless those tests run.
"""

import pytest

from src.orchestra_dbt.compatibility import check_dbt_core_version


def test_supported_dbt_core_installed():
    check_dbt_core_version()


def test_dbt_cli_runner_import():
    pytest.importorskip("dbt.cli.main")
    from dbt.cli.main import dbtRunner

    assert dbtRunner is not None


def test_freshness_runner_task_imports():
    pytest.importorskip("dbt.task.freshness")
    from dbt.task.freshness import FreshnessRunner, FreshnessTask

    assert FreshnessRunner is not None
    assert FreshnessTask is not None


def test_freshness_artifact_imports():
    pytest.importorskip("dbt.artifacts.schemas.freshness")
    from dbt.artifacts.schemas.freshness import SourceDefinition
    from dbt.artifacts.schemas.freshness.v3.freshness import SourceFreshnessResult
    from dbt.artifacts.schemas.results import FreshnessStatus

    assert SourceDefinition is not None
    assert SourceFreshnessResult is not None
    assert FreshnessStatus is not None
