from unittest.mock import patch

import pytest

from src.orchestra_dbt.constants import ORCHESTRA_REUSED_NODE
from src.orchestra_dbt.modify import (
    _build_generated_selector_definition,
    _split_selection_args,
    modify_dbt_command,
    update_selectors_yaml,
)

_REUSED_EXCLUSION_CRITERIA = {
    "method": "tag",
    "value": ORCHESTRA_REUSED_NODE,
    "indirect_selection": "cautious",
}


class TestUpdateSelectorsYaml:
    def test_update_selectors_yaml_success(self):
        selector_tag = "test_selector"
        original_selectors = {
            "selectors": [
                {"name": "other_selector", "definition": {"tag": "other"}},
                {"name": selector_tag, "definition": {"tag": "test"}},
            ]
        }

        with patch(
            "src.orchestra_dbt.modify.load_yaml", return_value=original_selectors
        ):
            with patch("src.orchestra_dbt.modify.save_yaml") as mock_save:
                result = update_selectors_yaml(selector_tag)

        assert result is True
        mock_save.assert_called_once()
        saved_data = mock_save.call_args[0][1]
        assert "selectors" in saved_data
        assert len(saved_data["selectors"]) == 3

        # Check that the original selector was renamed
        renamed_selector = saved_data["selectors"][1]
        assert renamed_selector["name"] != selector_tag
        assert "_" in renamed_selector["name"]  # UUID with underscores
        assert len(renamed_selector["name"]) == 36  # UUID length without dashes

        # Check that a new selector was added with intersection logic
        new_selector = saved_data["selectors"][2]
        assert new_selector == {
            "name": selector_tag,
            "definition": {
                "intersection": [
                    {"method": "selector", "value": renamed_selector["name"]},
                    {
                        "exclude": [
                            {
                                "method": "tag",
                                "value": ORCHESTRA_REUSED_NODE,
                                "indirect_selection": "cautious",
                            }
                        ]
                    },
                ]
            },
        }

    def test_update_selectors_yaml_file_not_found(self):
        with patch("src.orchestra_dbt.modify.load_yaml", side_effect=FileNotFoundError):
            with patch("src.orchestra_dbt.modify.log_error") as mock_log_error:
                result = update_selectors_yaml("test_selector")
        assert result is False
        mock_log_error.assert_called_once_with(
            "A `--selector` was used on the command, but no `selectors.yml` file found."
        )

    def test_update_selectors_yaml_no_selectors_key(self):
        with patch("src.orchestra_dbt.modify.load_yaml", return_value={}):
            with patch("src.orchestra_dbt.modify.log_error") as mock_log_error:
                result = update_selectors_yaml("test_selector")
        assert result is False
        mock_log_error.assert_called_once_with(
            "A `--selector` was used on the command, but no valid selectors found in `selectors.yml`."
        )

    def test_update_selectors_yaml_selectors_not_list(self):
        with patch(
            "src.orchestra_dbt.modify.load_yaml",
            return_value={"selectors": "not a list"},
        ):
            with patch("src.orchestra_dbt.modify.log_error") as mock_log_error:
                result = update_selectors_yaml("test_selector")
        assert result is False
        mock_log_error.assert_called_once_with(
            "A `--selector` was used on the command, but no valid selectors found in `selectors.yml`."
        )

    def test_update_selectors_yaml_empty_selectors_list(self):
        with patch(
            "src.orchestra_dbt.modify.load_yaml", return_value={"selectors": []}
        ):
            with patch("src.orchestra_dbt.modify.log_error") as mock_log_error:
                result = update_selectors_yaml("test_selector")
        assert result is False
        mock_log_error.assert_called_once_with(
            "A `--selector` was used on the command, but no valid selectors found in `selectors.yml`."
        )

    def test_update_selectors_yaml_selector_not_found(self):
        original_selectors = {
            "selectors": [
                {"name": "other_selector", "definition": {"tag": "other"}},
            ]
        }
        with patch(
            "src.orchestra_dbt.modify.load_yaml", return_value=original_selectors
        ):
            with patch("src.orchestra_dbt.modify.log_error") as mock_log_error:
                result = update_selectors_yaml("nonexistent_selector")
        assert result is False
        mock_log_error.assert_called_once_with(
            "Selector `nonexistent_selector` not found in `selectors.yml`."
        )

    def test_update_selectors_yaml_save_error(self):
        selector_tag = "test_selector"
        original_selectors = {
            "selectors": [
                {"name": selector_tag, "definition": {"tag": "test"}},
            ]
        }

        with patch(
            "src.orchestra_dbt.modify.load_yaml", return_value=original_selectors
        ):
            with patch(
                "src.orchestra_dbt.modify.save_yaml",
                side_effect=Exception("Save failed"),
            ):
                with patch("src.orchestra_dbt.modify.log_error") as mock_log_error:
                    result = update_selectors_yaml(selector_tag)
        assert result is False
        mock_log_error.assert_called_once_with(
            "Error saving selectors.yml: Save failed"
        )

    def test_update_selectors_yaml_preserves_other_selectors(self):
        selector_tag = "test_selector"
        original_selectors = {
            "selectors": [
                {"name": "selector_a", "definition": {"tag": "a"}},
                {"name": selector_tag, "definition": {"tag": "test"}},
                {"name": "selector_b", "definition": {"tag": "b"}},
            ]
        }

        with patch(
            "src.orchestra_dbt.modify.load_yaml", return_value=original_selectors
        ):
            with patch("src.orchestra_dbt.modify.save_yaml") as mock_save:
                result = update_selectors_yaml(selector_tag)

        assert result is True
        saved_data = mock_save.call_args[0][1]
        assert len(saved_data["selectors"]) == 4
        # Check that other selectors are preserved
        assert saved_data["selectors"][0]["name"] == "selector_a"
        assert saved_data["selectors"][2]["name"] == "selector_b"
        assert saved_data["selectors"][3]["name"] == selector_tag


class TestModifyDbtCommand:
    def test_modify_dbt_command_without_selector(self):
        assert modify_dbt_command(["dbt", "run"]) == [
            "dbt",
            "run",
            "--exclude",
            f"tag:{ORCHESTRA_REUSED_NODE}",
        ]

    @pytest.mark.parametrize("subcommand", ["build", "test"])
    def test_modify_dbt_command_adds_cautious_for_test_running_commands(
        self, subcommand
    ):
        assert modify_dbt_command(["dbt", subcommand]) == [
            "dbt",
            subcommand,
            "--exclude",
            f"tag:{ORCHESTRA_REUSED_NODE}",
            "--indirect-selection",
            "cautious",
        ]

    def test_bare_build_respects_user_indirect_selection(self):
        result = modify_dbt_command(["dbt", "build", "--indirect-selection", "eager"])
        assert result == [
            "dbt",
            "build",
            "--indirect-selection",
            "eager",
            "--exclude",
            f"tag:{ORCHESTRA_REUSED_NODE}",
        ]

    @pytest.mark.parametrize(
        "select_flag", ["--select", "-s", "--models", "--model", "-m"]
    )
    def test_user_select_is_folded_into_a_generated_selector(self, select_flag):
        with patch("src.orchestra_dbt.modify.load_yaml", side_effect=FileNotFoundError):
            with patch("src.orchestra_dbt.modify.save_yaml") as mock_save:
                result = modify_dbt_command(["dbt", "build", select_flag, "my_model+"])

        assert result[:2] == ["dbt", "build"]
        assert result[-2] == "--selector"
        name = result[-1]
        assert name.startswith("orchestra_reused_")
        assert select_flag not in result
        assert "my_model+" not in result

        saved_selector = mock_save.call_args[0][1]["selectors"][-1]
        assert saved_selector["name"] == name
        assert saved_selector["definition"] == {
            "union": [
                "my_model+",
                {"exclude": [_REUSED_EXCLUSION_CRITERIA]},
            ]
        }

    def test_user_exclude_is_folded_into_a_generated_selector(self):
        with patch("src.orchestra_dbt.modify.load_yaml", side_effect=FileNotFoundError):
            with patch("src.orchestra_dbt.modify.save_yaml") as mock_save:
                result = modify_dbt_command(
                    ["dbt", "build", "--exclude", "other_model"]
                )

        assert result[-2] == "--selector"
        saved_selector = mock_save.call_args[0][1]["selectors"][-1]
        assert saved_selector["definition"] == {
            "union": [
                "fqn:*",
                {"exclude": ["other_model", _REUSED_EXCLUSION_CRITERIA]},
            ]
        }

    def test_generated_selector_preserves_other_flags(self):
        with patch("src.orchestra_dbt.modify.load_yaml", side_effect=FileNotFoundError):
            with patch("src.orchestra_dbt.modify.save_yaml"):
                result = modify_dbt_command(
                    ["dbt", "build", "--select", "a", "b", "--threads", "4"]
                )
        assert result[:2] == ["dbt", "build"]
        assert "--threads" in result
        assert result[result.index("--threads") + 1] == "4"
        assert "--select" not in result

    def test_run_with_select_does_not_generate_selector(self):
        result = modify_dbt_command(["dbt", "run", "--select", "my_model"])
        assert result == [
            "dbt",
            "run",
            "--select",
            "my_model",
            "--exclude",
            f"tag:{ORCHESTRA_REUSED_NODE}",
        ]

    def test_falls_back_to_tag_exclusion_when_selector_cannot_be_written(self):
        with patch("src.orchestra_dbt.modify.load_yaml", side_effect=FileNotFoundError):
            with patch(
                "src.orchestra_dbt.modify.save_yaml", side_effect=OSError("disk full")
            ):
                with patch("src.orchestra_dbt.modify.log_warn"):
                    result = modify_dbt_command(["dbt", "build", "--select", "a"])
        assert result == [
            "dbt",
            "build",
            "--select",
            "a",
            "--exclude",
            f"tag:{ORCHESTRA_REUSED_NODE}",
        ]

    @pytest.mark.parametrize("success", [True, False])
    def test_modify_dbt_command_with_selector_success(self, success):
        cmd = ["dbt", "run", "--selector", "test_selector"]
        with patch(
            "src.orchestra_dbt.modify.update_selectors_yaml", return_value=success
        ) as mock_update:
            result = modify_dbt_command(cmd)
            assert result == cmd
        mock_update.assert_called_once_with("test_selector")

    def test_modify_dbt_command_with_selector_no_tag(self):
        with patch("src.orchestra_dbt.modify.log_error") as mock_log_error:
            modify_dbt_command(["dbt", "run", "--selector"])
        mock_log_error.assert_called_once_with(
            "A `--selector` was used on the command, but no selector tag provided."
        )


class TestSplitSelectionArgs:
    def test_no_selection_args(self):
        passthrough, includes, excludes = _split_selection_args(["--threads", "4"])
        assert passthrough == ["--threads", "4"]
        assert includes == []
        assert excludes == []

    def test_select_consumes_values_up_to_next_flag(self):
        passthrough, includes, excludes = _split_selection_args(
            ["--select", "a", "b", "--exclude", "c", "--threads", "4"]
        )
        assert includes == ["a", "b"]
        assert excludes == ["c"]
        assert passthrough == ["--threads", "4"]

    def test_equals_form(self):
        passthrough, includes, excludes = _split_selection_args(
            ["--select=my_model+", "--exclude=other"]
        )
        assert includes == ["my_model+"]
        assert excludes == ["other"]
        assert passthrough == []

    def test_select_aliases(self):
        _, includes, _ = _split_selection_args(["-s", "a", "-m", "b", "--models", "c"])
        assert includes == ["a", "b", "c"]

    def test_whitespace_separated_values_in_one_token_are_split(self):
        _, includes, _ = _split_selection_args(["--select", "model_a+ model_b"])
        assert includes == ["model_a+", "model_b"]
        _, includes_eq, _ = _split_selection_args(["--select=model_a model_b"])
        assert includes_eq == ["model_a", "model_b"]

    def test_exclude_resource_types_is_not_treated_as_exclude(self):
        passthrough, includes, excludes = _split_selection_args(
            ["--exclude-resource-types", "test"]
        )
        assert excludes == []
        assert passthrough == ["--exclude-resource-types", "test"]


class TestBuildGeneratedSelectorDefinition:
    def test_includes_and_excludes(self):
        assert _build_generated_selector_definition(["a+", "b"], ["c"]) == {
            "union": [
                "a+",
                "b",
                {"exclude": ["c", _REUSED_EXCLUSION_CRITERIA]},
            ]
        }

    def test_no_includes_defaults_to_everything(self):
        assert _build_generated_selector_definition([], ["c"]) == {
            "union": [
                "fqn:*",
                {"exclude": ["c", _REUSED_EXCLUSION_CRITERIA]},
            ]
        }
