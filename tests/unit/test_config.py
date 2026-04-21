from pathlib import Path

import pytest

from src.orchestra_dbt.config import effective_state_file_path, find_pyproject_directory


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
