from pathlib import Path

import pytest

from src.orchestra_dbt.config import (
    get_orchestra_api_key,
    get_orchestra_state_file_env_override,
    load_orchestra_dbt_settings,
)
from src.orchestra_dbt.project_discovery import find_orchestra_dbt_config
from src.orchestra_dbt.state_backends import resolve_state_backend_config
from src.orchestra_dbt.state_types import StateBackendKind


def test_get_orchestra_api_key_strips_and_none_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    assert get_orchestra_api_key() is None
    monkeypatch.setenv("ORCHESTRA_API_KEY", "  secret  ")
    assert get_orchestra_api_key() == "secret"


def test_get_orchestra_state_file_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ORCHESTRA_STATE_FILE", raising=False)
    assert get_orchestra_state_file_env_override() is None
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", " /tmp/state.json ")
    assert get_orchestra_state_file_env_override() == "/tmp/state.json"


def _backend(project_dir: Path):
    return resolve_state_backend_config(load_orchestra_dbt_settings(project_dir))


def _clear_orchestra_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "ORCHESTRA_ENV",
        "ORCHESTRA_USE_STATEFUL",
        "ORCHESTRA_LOCAL_RUN",
        "ORCHESTRA_DBT_DEBUG",
        "ORCHESTRA_INTEGRATION_ACCOUNT_ID",
        "ORCHESTRA_SEED_STATE_ORCHESTRATION",
        "ORCHESTRA_REQUIRE_EXPLICIT_SOURCE_FRESHNESS",
        "ORCHESTRA_VERIFY_RELATIONS_EXIST",
        "ORCHESTRA_SCOPE_SOURCE_FRESHNESS_TO_SELECTION",
        "ORCHESTRA_DEFAULT_LAG_TOLERANCE",
    ):
        monkeypatch.delenv(key, raising=False)


def test_state_file_path_env_overrides_pyproject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.orchestra_dbt]\nstate_file = "from_pyproject.json"\n',
        encoding="utf-8",
    )
    other = tmp_path / "from_env.json"
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", str(other))
    assert _backend(tmp_path).local_path == other.resolve()


def test_state_file_path_env_relative_to_cwd(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.delenv("ORCHESTRA_STATE_FILE", raising=False)
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", "rel/state.json")
    assert _backend(tmp_path).local_path == (tmp_path / "rel" / "state.json").resolve()


def test_state_file_path_none_without_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.delenv("ORCHESTRA_STATE_FILE", raising=False)
    assert _backend(tmp_path).local_path is None


def test_resolve_state_backend_config_s3_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", "s3://my-bucket/prefix/state.json")
    cfg = _backend(tmp_path)
    assert cfg.kind == StateBackendKind.S3
    assert cfg.s3_bucket == "my-bucket"
    assert cfg.s3_key == "prefix/state.json"


def test_resolve_state_backend_config_s3_from_pyproject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.orchestra_dbt]\nstate_file = "s3://b/k/state.json"\n',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.delenv("ORCHESTRA_STATE_FILE", raising=False)
    cfg = _backend(tmp_path)
    assert cfg.kind == StateBackendKind.S3
    assert cfg.s3_bucket == "b"
    assert cfg.s3_key == "k/state.json"


def test_state_file_path_api_key_prefers_http_over_env_and_pyproject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.orchestra_dbt]\nstate_file = "from_pyproject.json"\n',
        encoding="utf-8",
    )
    other = tmp_path / "from_env.json"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ORCHESTRA_API_KEY", "secret")
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", str(other))
    assert _backend(tmp_path).local_path is None


def test_load_orchestra_dbt_settings_defaults(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    _clear_orchestra_settings_env(monkeypatch)
    settings = load_orchestra_dbt_settings(tmp_path)
    assert settings.use_stateful is False
    assert settings.orchestra_env == "app"
    assert settings.local_run is True
    assert settings.debug is False
    assert settings.integration_account_id is None
    assert settings.seed_state_orchestration is False
    assert settings.require_explicit_source_freshness is False
    assert settings.verify_relations_exist is False
    assert settings.scope_source_freshness_to_selection is False
    assert settings.default_lag_tolerance is None


def test_load_orchestra_dbt_settings_from_pyproject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """[tool.orchestra_dbt]
use_stateful = true
orchestra_env = "stage"
local_run = true
debug = true
integration_account_id = "acct-from-toml"
seed_state_orchestration = true
require_explicit_source_freshness = true
verify_relations_exist = true
scope_source_freshness_to_selection = true
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    _clear_orchestra_settings_env(monkeypatch)
    settings = load_orchestra_dbt_settings(tmp_path)
    assert settings.use_stateful is True
    assert settings.orchestra_env == "stage"
    assert settings.local_run is True
    assert settings.debug is True
    assert settings.integration_account_id == "acct-from-toml"
    assert settings.seed_state_orchestration is True
    assert settings.require_explicit_source_freshness is True
    assert settings.verify_relations_exist is True
    assert settings.scope_source_freshness_to_selection is True


def test_load_orchestra_dbt_settings_env_overrides_pyproject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.orchestra_dbt]\nuse_stateful = true\norchestra_env = "stage"\n'
        'integration_account_id = "from-toml"\n'
        "seed_state_orchestration = false\n"
        "require_explicit_source_freshness = false\n"
        "verify_relations_exist = true\n"
        "scope_source_freshness_to_selection = false\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ORCHESTRA_USE_STATEFUL", "false")
    monkeypatch.setenv("ORCHESTRA_ENV", "dev")
    monkeypatch.setenv("ORCHESTRA_INTEGRATION_ACCOUNT_ID", "from-env")
    monkeypatch.setenv("ORCHESTRA_SEED_STATE_ORCHESTRATION", "true")
    monkeypatch.setenv("ORCHESTRA_REQUIRE_EXPLICIT_SOURCE_FRESHNESS", "true")
    monkeypatch.setenv("ORCHESTRA_VERIFY_RELATIONS_EXIST", "false")
    monkeypatch.setenv("ORCHESTRA_SCOPE_SOURCE_FRESHNESS_TO_SELECTION", "true")
    settings = load_orchestra_dbt_settings(tmp_path)
    assert settings.use_stateful is False
    assert settings.orchestra_env == "dev"
    assert settings.integration_account_id == "from-env"
    assert settings.seed_state_orchestration is True
    assert settings.require_explicit_source_freshness is True
    assert settings.verify_relations_exist is False
    assert settings.scope_source_freshness_to_selection is True


def test_load_orchestra_dbt_settings_invalid_orchestra_env_in_pyproject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.orchestra_dbt]\norchestra_env = "invalid"\n',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    _clear_orchestra_settings_env(monkeypatch)
    with pytest.raises(ValueError, match="Invalid"):
        load_orchestra_dbt_settings(tmp_path)


def test_load_orchestra_dbt_settings_invalid_orchestra_env_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.orchestra_dbt]\norchestra_env = "dev"\n',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ORCHESTRA_ENV", "invalid")
    with pytest.raises(ValueError, match="Invalid"):
        load_orchestra_dbt_settings(tmp_path)


def _write_settings(directory: Path, section: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pyproject.toml").write_text(section, encoding="utf-8")


def test_settings_come_from_project_dir_before_cwd(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _write_settings(tmp_path, "[tool.orchestra_dbt]\nuse_stateful = false\n")
    _write_settings(tmp_path / "proj", "[tool.orchestra_dbt]\nuse_stateful = true\n")
    monkeypatch.chdir(tmp_path)
    _clear_orchestra_settings_env(monkeypatch)
    settings = load_orchestra_dbt_settings(Path("proj"))
    assert settings.use_stateful is True
    assert settings.pyproject_dir == (tmp_path / "proj").resolve()


def test_settings_walk_up_from_project_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _write_settings(tmp_path / "repo", "[tool.orchestra_dbt]\nuse_stateful = true\n")
    (tmp_path / "repo" / "dbt").mkdir()
    monkeypatch.chdir(tmp_path)
    _clear_orchestra_settings_env(monkeypatch)
    settings = load_orchestra_dbt_settings(tmp_path / "repo" / "dbt")
    assert settings.use_stateful is True
    assert settings.pyproject_dir == (tmp_path / "repo").resolve()


def test_settings_fall_back_to_cwd(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "proj").mkdir()
    _write_settings(tmp_path / "cwd", "[tool.orchestra_dbt]\nuse_stateful = true\n")
    monkeypatch.chdir(tmp_path / "cwd")
    _clear_orchestra_settings_env(monkeypatch)
    settings = load_orchestra_dbt_settings(tmp_path / "proj")
    assert settings.use_stateful is True
    assert settings.pyproject_dir == (tmp_path / "cwd").resolve()


def test_pyproject_without_section_does_not_hide_parent_settings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _write_settings(tmp_path, "[tool.orchestra_dbt]\nuse_stateful = true\n")
    _write_settings(tmp_path / "proj", "[project]\nname = 'dbt-deps'\n")
    monkeypatch.chdir(tmp_path)
    _clear_orchestra_settings_env(monkeypatch)
    settings = load_orchestra_dbt_settings(Path("proj"))
    assert settings.use_stateful is True
    assert find_orchestra_dbt_config(tmp_path / "proj") == (
        tmp_path.resolve(),
        {"use_stateful": True},
    )


def test_pyproject_state_file_resolves_from_its_own_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _write_settings(
        tmp_path / "proj", '[tool.orchestra_dbt]\nstate_file = "state.json"\n'
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.delenv("ORCHESTRA_STATE_FILE", raising=False)
    assert (
        _backend(Path("proj")).local_path
        == (tmp_path / "proj" / "state.json").resolve()
    )


def test_env_state_file_resolves_from_cwd_not_project_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _write_settings(
        tmp_path / "proj", '[tool.orchestra_dbt]\nstate_file = "state.json"\n'
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", "env_state.json")
    assert _backend(Path("proj")).local_path == (tmp_path / "env_state.json").resolve()


def test_default_lag_tolerance_from_pyproject_and_env_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """[tool.orchestra_dbt]
default_lag_tolerance = "45m"
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    _clear_orchestra_settings_env(monkeypatch)
    assert load_orchestra_dbt_settings(tmp_path).default_lag_tolerance == "45m"

    monkeypatch.setenv("ORCHESTRA_DEFAULT_LAG_TOLERANCE", " 2h ")
    assert load_orchestra_dbt_settings(tmp_path).default_lag_tolerance == "2h"


def test_default_lag_tolerance_invalid_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    _clear_orchestra_settings_env(monkeypatch)
    monkeypatch.setenv("ORCHESTRA_DEFAULT_LAG_TOLERANCE", "soon")
    with pytest.raises(ValueError, match="lag_tolerance"):
        load_orchestra_dbt_settings(tmp_path)
