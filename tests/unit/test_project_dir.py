"""Running from outside the dbt project: project-relative paths resolve via --project-dir."""

from hashlib import sha256

import pytest
import yaml

from src.orchestra_dbt.checksum import calculate_checksum
from src.orchestra_dbt.config import load_orchestra_dbt_settings
from src.orchestra_dbt.constants import ORCHESTRA_REUSED_NODE
from src.orchestra_dbt.models import Freshness, FreshnessConfig, MaterialisationNode
from src.orchestra_dbt.modify import (
    modify_dbt_command,
    restore_selectors_file,
    snapshot_selectors_file,
)
from src.orchestra_dbt.patcher import (
    patch_seed_properties,
    patch_sql_files,
    revert_patching,
)
from src.orchestra_dbt.target_finder import (
    artifact_path,
    project_path,
    resolve_artifact_dir,
)


@pytest.fixture
def project(tmp_path, monkeypatch):
    """cwd is tmp_path; the dbt project is tmp_path/proj, passed as a relative --project-dir."""
    for name in ("PROJECT_DIR", "TARGET_PATH"):
        monkeypatch.delenv(f"DBT_{name}", raising=False)
        monkeypatch.delenv(f"DBT_ENGINE_{name}", raising=False)
    monkeypatch.chdir(tmp_path)
    proj = tmp_path / "proj"
    (proj / "models").mkdir(parents=True)
    (proj / "seeds").mkdir()
    resolve_artifact_dir(["dbt", "build", "--project-dir", "proj"])
    return proj


def _node(file_path: str) -> MaterialisationNode:
    return MaterialisationNode(
        asset_external_id="x",
        checksum="c",
        dbt_path=file_path,
        file_path=file_path,
        freshness_config=FreshnessConfig(),
        freshness=Freshness.CLEAN,
        sources={},
        reason="same state",
    )


def test_project_path_follows_project_dir_env(monkeypatch):
    monkeypatch.delenv("DBT_ENGINE_PROJECT_DIR", raising=False)
    monkeypatch.setenv("DBT_PROJECT_DIR", "from_env")
    resolve_artifact_dir(["dbt", "build"])
    assert str(project_path("seeds/a.csv")) == "from_env/seeds/a.csv"
    resolve_artifact_dir(["dbt", "build", "--project-dir=flag"])
    assert str(project_path("seeds/a.csv")) == "flag/seeds/a.csv"


def test_artifacts_are_under_the_project_dir(project):
    assert artifact_path("manifest.json") == "proj/target/manifest.json"


def test_artifact_target_path_rules(monkeypatch):
    for name in ("PROJECT_DIR", "TARGET_PATH"):
        monkeypatch.delenv(f"DBT_{name}", raising=False)
    monkeypatch.setenv("DBT_ENGINE_TARGET_PATH", "engine")
    monkeypatch.setenv("DBT_TARGET_PATH", "plain")
    resolve_artifact_dir(["dbt", "build", "--project-dir", "proj"])
    assert artifact_path("x") == "proj/engine/x"
    resolve_artifact_dir(
        ["dbt", "build", "--project-dir", "proj", "--target-path=/abs"]
    )
    assert artifact_path("x") == "/abs/x"


def test_seed_checksum_reads_from_project_dir(project):
    (project / "seeds" / "raw_events.csv").write_bytes(b"id\n1\n")
    assert (
        calculate_checksum("seed", "manifest", "seeds/raw_events.csv")
        == sha256(b"id\n1\n").hexdigest()
    )


def test_sql_patch_and_revert_in_project_dir(project):
    sql = project / "models" / "a.sql"
    sql.write_text("select 1", encoding="utf-8")

    patch_sql_files({"a": _node("models/a.sql")})
    assert ORCHESTRA_REUSED_NODE in sql.read_text(encoding="utf-8")

    revert_patching(["models/a.sql"])
    assert sql.read_text(encoding="utf-8") == "select 1"


def test_seed_properties_written_in_project_dir(project, tmp_path):
    patch_seed_properties({"s": _node("seeds/raw_events.csv")})
    assert not (tmp_path / "seeds").exists()
    props = yaml.safe_load((project / "seeds" / "properties.yml").read_text())
    assert props["seeds"][0]["name"] == "raw_events"


def test_selectors_file_in_project_dir(project, tmp_path):
    snapshot = snapshot_selectors_file()
    cmd = modify_dbt_command(["dbt", "build", "--select", "a"])
    assert not (tmp_path / "selectors.yml").exists()
    selectors = yaml.safe_load((project / "selectors.yml").read_text())
    assert cmd[-1] == selectors["selectors"][0]["name"]

    restore_selectors_file(snapshot)
    assert not (project / "selectors.yml").exists()


def test_settings_found_in_project_pyproject(project, monkeypatch):
    monkeypatch.delenv("ORCHESTRA_INTEGRATION_ACCOUNT_ID", raising=False)
    (project / "pyproject.toml").write_text(
        '[tool.orchestra_dbt]\nintegration_account_id = "from-project"\n'
    )
    assert load_orchestra_dbt_settings().integration_account_id == "from-project"


def test_settings_fall_back_to_cwd_pyproject(project, tmp_path, monkeypatch):
    monkeypatch.delenv("ORCHESTRA_INTEGRATION_ACCOUNT_ID", raising=False)
    outside = tmp_path.parent / f"{tmp_path.name}_outside"
    outside.mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.orchestra_dbt]\nintegration_account_id = "from-cwd"\n'
    )
    resolve_artifact_dir(["dbt", "build", "--project-dir", str(outside)])
    assert load_orchestra_dbt_settings().integration_account_id == "from-cwd"
