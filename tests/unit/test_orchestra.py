from unittest.mock import patch

from src.orchestra_dbt.orchestra import is_warn


class TestIsWarn:
    def test_file_not_found(self, capsys):
        with (
            patch("src.orchestra_dbt.orchestra.load_json") as mock_load_json,
        ):
            mock_load_json.side_effect = FileNotFoundError("File not found")
            is_warn()
            assert capsys.readouterr().out.strip() == "SUCCEEDED"

    def test_file_malformed(self, capsys):
        with (
            patch("src.orchestra_dbt.orchestra.load_json") as mock_load_json,
        ):
            mock_load_json.side_effect = ValueError("Invalid JSON")
            is_warn()
            assert capsys.readouterr().out.strip() == "SUCCEEDED"

    def test_no_results_empty_array(self, capsys):
        with patch("src.orchestra_dbt.orchestra.load_json") as mock_load_json:
            mock_load_json.return_value = {"results": []}
            is_warn()
            assert capsys.readouterr().out.strip() == "SUCCEEDED"

    def test_one_warning(self, capsys):
        with patch("src.orchestra_dbt.orchestra.load_json") as mock_load_json:
            mock_load_json.return_value = {
                "results": [
                    {"status": "success"},
                    {"status": "warn"},
                    {"status": "success"},
                ]
            }
            is_warn()
            assert capsys.readouterr().out.strip() == "WARNING"

    def test_no_warnings(self, capsys):
        with patch("src.orchestra_dbt.orchestra.load_json") as mock_load_json:
            mock_load_json.return_value = {
                "results": [
                    {"status": "success"},
                    {"status": "success"},
                    {"status": "error"},
                ]
            }
            is_warn()
            assert capsys.readouterr().out.strip() == "SUCCEEDED"
