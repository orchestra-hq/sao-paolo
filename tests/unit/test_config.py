from pathlib import Path

import pytest

from src.orchestra_dbt.config import (
    effective_state_file_path,
    effective_state_persistence,
    get_integration_account_id,
    get_orchestra_api_key,
    get_orchestra_state_file_env_override,
    load_orchestra_dbt_settings,
    resolve_state_backend_config,
)
from src.orchestra_dbt.project_discovery import find_pyproject_directory
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


def _clear_orchestra_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "ORCHESTRA_ENV",
        "ORCHESTRA_USE_STATEFUL",
        "ORCHESTRA_LOCAL_RUN",
        "ORCHESTRA_DBT_DEBUG",
        "ORCHESTRA_INTEGRATION_ACCOUNT_ID",
        "ORCHESTRA_SEED_STATE_ORCHESTRATION",
    ):
        monkeypatch.delenv(key, raising=False)


def test_effective_state_file_path_env_overrides_pyproject(
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
    assert effective_state_file_path() == other.resolve()


def test_effective_state_file_path_env_relative_to_cwd(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.delenv("ORCHESTRA_STATE_FILE", raising=False)
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", "rel/state.json")
    assert effective_state_file_path() == (tmp_path / "rel" / "state.json").resolve()


def test_find_pyproject_directory_walks_up(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname = 'x'\n", encoding="utf-8"
    )
    sub = tmp_path / "a" / "b"
    sub.mkdir(parents=True)
    monkeypatch.chdir(sub)
    assert find_pyproject_directory() == tmp_path.resolve()


def test_effective_state_file_path_none_without_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.delenv("ORCHESTRA_STATE_FILE", raising=False)
    assert effective_state_file_path() is None


def test_resolve_state_backend_config_s3_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", "s3://my-bucket/prefix/state.json")
    cfg = resolve_state_backend_config()
    assert cfg.kind == StateBackendKind.S3
    assert cfg.s3_bucket == "my-bucket"
    assert cfg.s3_key == "prefix/state.json"


def test_effective_state_persistence_matches_resolve_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", "s3://my-bucket/prefix/state.json")
    assert (
        effective_state_persistence().model_dump()
        == resolve_state_backend_config().model_dump()
    )


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
    cfg = resolve_state_backend_config()
    assert cfg.kind == StateBackendKind.S3
    assert cfg.s3_bucket == "b"
    assert cfg.s3_key == "k/state.json"


def test_effective_state_file_path_api_key_prefers_http_over_env_and_pyproject(
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
    assert effective_state_file_path() is None


def test_load_orchestra_dbt_settings_defaults(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    _clear_orchestra_settings_env(monkeypatch)
    settings = load_orchestra_dbt_settings()
    assert settings.use_stateful is False
    assert settings.orchestra_env == "app"
    assert settings.local_run is True
    assert settings.debug is False
    assert settings.integration_account_id is None
    assert settings.seed_state_orchestration is False


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
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    _clear_orchestra_settings_env(monkeypatch)
    settings = load_orchestra_dbt_settings()
    assert settings.use_stateful is True
    assert settings.orchestra_env == "stage"
    assert settings.local_run is True
    assert settings.debug is True
    assert settings.integration_account_id == "acct-from-toml"
    assert settings.seed_state_orchestration is True
    assert get_integration_account_id() == "acct-from-toml"


def test_load_orchestra_dbt_settings_env_overrides_pyproject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.orchestra_dbt]\nuse_stateful = true\norchestra_env = "stage"\n'
        'integration_account_id = "from-toml"\n'
        "seed_state_orchestration = false\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ORCHESTRA_USE_STATEFUL", "false")
    monkeypatch.setenv("ORCHESTRA_ENV", "dev")
    monkeypatch.setenv("ORCHESTRA_INTEGRATION_ACCOUNT_ID", "from-env")
    monkeypatch.setenv("ORCHESTRA_SEED_STATE_ORCHESTRATION", "true")
    settings = load_orchestra_dbt_settings()
    assert settings.use_stateful is False
    assert settings.orchestra_env == "dev"
    assert settings.integration_account_id == "from-env"
    assert settings.seed_state_orchestration is True


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
        load_orchestra_dbt_settings()


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
        load_orchestra_dbt_settings()
