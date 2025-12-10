from unittest.mock import patch

import pytest

from src.orchestra_dbt.constants import ORCHESTRA_REUSED_NODE
from src.orchestra_dbt.modify import modify_dbt_command, update_selectors_yaml


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
                    {"exclude": [{"method": "tag", "value": ORCHESTRA_REUSED_NODE}]},
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
