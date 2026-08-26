import pytest

from src.orchestra_dbt.full_refresh_finder import is_full_refresh_requested


@pytest.fixture(autouse=True)
def _no_dbt_full_refresh_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DBT_FULL_REFRESH", raising=False)


class TestIsFullRefreshRequested:
    """Unlike `--target`, this is a plain boolean flag with no value (click rejects
    `--full-refresh=true` outright), so only flag presence and the env var matter here.
    """

    def test_absent_by_default(self) -> None:
        assert is_full_refresh_requested(["dbt", "build"]) is False

    def test_long_flag(self) -> None:
        assert is_full_refresh_requested(["dbt", "build", "--full-refresh"]) is True

    def test_short_flag(self) -> None:
        assert is_full_refresh_requested(["dbt", "build", "-f"]) is True

    @pytest.mark.parametrize("value", ["true", "TRUE", " yes ", "1", "on", "t", "y"])
    def test_env_var_true_states(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("DBT_FULL_REFRESH", value)
        assert is_full_refresh_requested(["dbt", "build"]) is True

    @pytest.mark.parametrize("value", ["false", "0", "off", "f", "n", "no", ""])
    def test_env_var_false_states(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("DBT_FULL_REFRESH", value)
        assert is_full_refresh_requested(["dbt", "build"]) is False

    def test_unrecognised_env_var_value_fails_toward_not_requested(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Real dbt/click would reject this outright; we fail toward not-requested instead."""
        monkeypatch.setenv("DBT_FULL_REFRESH", "garbage")
        assert is_full_refresh_requested(["dbt", "build"]) is False

    def test_flag_beats_a_false_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DBT_FULL_REFRESH", "false")
        assert is_full_refresh_requested(["dbt", "build", "--full-refresh"]) is True

    def test_no_env_var_set_is_not_requested(self) -> None:
        assert is_full_refresh_requested(["dbt", "build"]) is False
