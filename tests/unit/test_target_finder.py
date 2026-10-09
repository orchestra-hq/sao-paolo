from pathlib import Path

import pytest

from src.orchestra_dbt.target_finder import (
    find_flag_value,
    find_target_dir,
    find_target_in_args,
)


@pytest.fixture(autouse=True)
def _no_dbt_target_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DBT_TARGET", raising=False)


class TestFindTargetInArgs:
    """Every case here was checked against real click (`--target`/`-t` is dbt's own option,
    unmodified), including the two sharp edges: `-t` attaches with no `=` splitting, and a
    repeated flag resolves to the last occurrence.
    """

    def test_find_target_in_args_success(self):
        args = ["dbt", "source", "freshness", "--target", "test"]
        assert find_target_in_args(args) == "test"

    def test_find_target_in_args_no_target(self):
        args = ["dbt", "source", "freshness"]
        assert find_target_in_args(args) is None

    @pytest.mark.parametrize(
        "args",
        [
            ["dbt", "build", "--target=prod"],
            ["dbt", "build", "-t", "prod"],
            ["dbt", "build", "-tprod"],
            ["dbt", "build", "--target", "prod", "--select", "my_model"],
            ["dbt", "build", "--select", "my_model", "--target=prod"],
        ],
    )
    def test_all_forms_dbt_accepts(self, args: list[str]) -> None:
        assert find_target_in_args(args) == "prod"

    def test_short_form_does_not_split_on_equals(self) -> None:
        """`-t=prod` resolves to the literal target `=prod`, matching click."""
        assert find_target_in_args(["dbt", "build", "-t=prod"]) == "=prod"

    def test_repeated_flag_resolves_to_the_last_occurrence(self) -> None:
        assert (
            find_target_in_args(["dbt", "build", "--target", "dev", "--target", "prod"])
            == "prod"
        )
        assert (
            find_target_in_args(["dbt", "build", "--target=dev", "--target=prod"])
            == "prod"
        )

    def test_falls_back_to_dbt_target_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DBT_TARGET", "from-env")
        assert find_target_in_args(["dbt", "build"]) == "from-env"

    def test_explicit_flag_beats_the_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DBT_TARGET", "from-env")
        assert find_target_in_args(["dbt", "build", "--target=prod"]) == "prod"

    def test_trailing_flag_with_no_value_falls_through(self) -> None:
        assert find_target_in_args(["dbt", "build", "--target"]) is None

    def test_flag_followed_by_another_flag_is_still_consumed_as_the_value(self) -> None:
        """click consumes the very next token unconditionally, even if it's itself a flag."""
        assert (
            find_target_in_args(["dbt", "build", "--target", "--select"]) == "--select"
        )

    def test_empty_env_var_is_treated_as_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """click's one special case: an empty (not whitespace) env var means unset."""
        monkeypatch.setenv("DBT_TARGET", "")
        assert find_target_in_args(["dbt", "build"]) is None

    def test_whitespace_only_env_var_is_a_literal_target(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unlike an empty string, click passes whitespace through as a real value."""
        monkeypatch.setenv("DBT_TARGET", "  ")
        assert find_target_in_args(["dbt", "build"]) == "  "

    def test_cli_values_are_never_stripped(self) -> None:
        """click passes CLI-supplied values through raw, even empty or whitespace-only."""
        assert find_target_in_args(["dbt", "build", "--target", ""]) == ""
        assert find_target_in_args(["dbt", "build", "--target", "  "]) == "  "

    def test_cli_empty_value_does_not_fall_back_to_the_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only an *unset* env var is a fallback trigger -- an explicit empty CLI value is
        itself the resolved value, exactly as click resolves it.
        """
        monkeypatch.setenv("DBT_TARGET", "env-fallback")
        assert find_target_in_args(["dbt", "build", "--target", ""]) == ""


class TestFindFlagValue:
    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (["dbt", "build", "--profile", "a"], "a"),
            (["dbt", "build", "--profile=a"], "a"),
            (["dbt", "build", "--profile", "a", "--profile=b"], "b"),
            (["dbt", "build", "--profile=a", "--profile", "b"], "b"),
            (["dbt", "build", "--profile="], ""),
            (["dbt", "build", "--profile"], None),
            (["dbt", "build", "--profiles-dir", "x"], None),
            (["dbt", "build"], None),
        ],
    )
    def test_matches_click(self, args: list[str], expected: str | None) -> None:
        assert find_flag_value(args, "--profile") == expected


class TestFindTargetDir:
    @pytest.fixture(autouse=True)
    def _no_target_path_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DBT_TARGET_PATH", raising=False)
        monkeypatch.delenv("DBT_ENGINE_TARGET_PATH", raising=False)

    def test_defaults_to_target_under_the_project(self):
        assert find_target_dir(["dbt", "build"], Path("proj")) == Path("proj/target")

    def test_flag_beats_engine_env_beats_plain_env(self, monkeypatch):
        monkeypatch.setenv("DBT_TARGET_PATH", "plain")
        assert find_target_dir([], Path("proj")) == Path("proj/plain")
        monkeypatch.setenv("DBT_ENGINE_TARGET_PATH", "engine")
        assert find_target_dir([], Path("proj")) == Path("proj/engine")
        assert find_target_dir(["--target-path=flag"], Path("proj")) == Path(
            "proj/flag"
        )

    def test_absolute_target_path_ignores_the_project(self):
        assert find_target_dir(["--target-path", "/abs"], Path("proj")) == Path("/abs")
