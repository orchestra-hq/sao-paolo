from unittest.mock import patch

from src.orchestra_dbt.patcher import _patch_file, patch_sql_files


class TestPatchFile:
    """Tests for _patch_file function."""

    def test_patch_file_adds_tag_when_no_config(self, tmp_path):
        """Test that tag is added when no config exists."""
        sql_file = tmp_path / "model.sql"
        original_content = "SELECT * FROM table"
        sql_file.write_text(original_content)

        _patch_file(sql_file, "run")

        content = sql_file.read_text()
        assert '{{ config(tags=["run"]) }}' in content
        assert original_content in content

    def test_patch_file_replaces_existing_config(self, tmp_path):
        """Test that existing config is replaced."""
        sql_file = tmp_path / "model.sql"
        original_content = '{{ config(tags=["old_tag"]) }}\nSELECT * FROM table'
        sql_file.write_text(original_content)

        _patch_file(sql_file, "run")

        content = sql_file.read_text()
        assert '{{ config(tags=["run"]) }}' in content
        assert "old_tag" not in content
        assert "SELECT * FROM table" in content

    def test_patch_file_handles_multiline_config(self, tmp_path):
        """Test that multiline config is handled correctly."""
        sql_file = tmp_path / "model.sql"
        original_content = """{{ config(
    tags=["old_tag"]
) }}
SELECT * FROM table"""
        sql_file.write_text(original_content)

        _patch_file(sql_file, "run")

        content = sql_file.read_text()
        assert '{{ config(tags=["run"]) }}' in content
        assert "old_tag" not in content

    def test_patch_file_preserves_content_after_config(self, tmp_path):
        """Test that content after config is preserved."""
        sql_file = tmp_path / "model.sql"
        original_content = '{{ config(tags=["old"]) }}\n-- Comment\nSELECT * FROM table'
        sql_file.write_text(original_content)

        _patch_file(sql_file, "run")

        content = sql_file.read_text()
        assert "-- Comment" in content
        assert "SELECT * FROM table" in content


class TestPatchSqlFiles:
    """Tests for patch_sql_files function."""

    def test_patch_sql_files_finds_sql_files(self, tmp_path):
        """Test that patch_sql_files finds SQL files in models directory."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        sql_file = models_dir / "model_a.sql"
        sql_file.write_text("SELECT * FROM table")

        with patch("orchestra_dbt.patcher.os.getcwd", return_value=str(tmp_path)):
            with patch("orchestra_dbt.patcher._patch_file") as mock_patch:
                patch_sql_files(["models/model_a.sql"])

                mock_patch.assert_called_once()
                call_args = mock_patch.call_args[0]
                assert "model_a.sql" in str(call_args[0])

    def test_patch_sql_files_applies_run_tag(self, tmp_path):
        """Test that files to run get 'run' tag."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        sql_file = models_dir / "model_a.sql"
        sql_file.write_text("SELECT * FROM table")

        with patch("orchestra_dbt.patcher.os.getcwd", return_value=str(tmp_path)):
            with patch("orchestra_dbt.patcher._patch_file") as mock_patch:
                patch_sql_files(["models/model_a.sql"])

                mock_patch.assert_called_with(sql_file, "run")

    def test_patch_sql_files_applies_reuse_tag(self, tmp_path):
        """Test that files not to run get 'reuse' tag."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        sql_file_a = models_dir / "model_a.sql"
        sql_file_a.write_text("SELECT * FROM table")
        sql_file_b = models_dir / "model_b.sql"
        sql_file_b.write_text("SELECT * FROM other_table")

        with patch("orchestra_dbt.patcher.os.getcwd", return_value=str(tmp_path)):
            with patch("orchestra_dbt.patcher._patch_file") as mock_patch:
                patch_sql_files(["models/model_a.sql"])

                # Should patch both files
                assert mock_patch.call_count == 2
                # model_b should get reuse tag
                calls = [call[0][1] for call in mock_patch.call_args_list]
                assert "reuse" in calls

    def test_patch_sql_files_ignores_non_models(self, tmp_path):
        """Test that files outside models directory are not patched."""
        other_dir = tmp_path / "other"
        other_dir.mkdir()

        sql_file = other_dir / "other.sql"
        sql_file.write_text("SELECT * FROM table")

        with patch("orchestra_dbt.patcher.os.getcwd", return_value=str(tmp_path)):
            with patch("orchestra_dbt.patcher._patch_file") as mock_patch:
                patch_sql_files(["models/model_a.sql"])

                # Should not patch files outside models
                assert mock_patch.call_count == 0

    def test_patch_sql_files_handles_relative_paths(self, tmp_path):
        """Test that relative paths work correctly."""
        models_dir = tmp_path / "models" / "staging"
        models_dir.mkdir(parents=True)

        sql_file = models_dir / "stg_model.sql"
        sql_file.write_text("SELECT * FROM table")

        with patch("orchestra_dbt.patcher.os.getcwd", return_value=str(tmp_path)):
            with patch("orchestra_dbt.patcher._patch_file") as mock_patch:
                patch_sql_files(["models/staging/stg_model.sql"])

                mock_patch.assert_called_once()

    @patch("orchestra_dbt.patcher.log_warn")
    def test_patch_sql_files_no_sql_files(self, mock_log_warn, tmp_path):
        """Test handling when no SQL files are found."""
        with patch("orchestra_dbt.patcher.os.getcwd", return_value=str(tmp_path)):
            patch_sql_files([])

            mock_log_warn.assert_called_once()

    @patch("orchestra_dbt.patcher.log_warn")
    def test_patch_sql_files_handles_exceptions(self, mock_log_warn, tmp_path):
        """Test that exceptions during patching are handled gracefully."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        sql_file = models_dir / "model_a.sql"
        sql_file.write_text("SELECT * FROM table")

        with patch("orchestra_dbt.patcher.os.getcwd", return_value=str(tmp_path)):
            with patch(
                "orchestra_dbt.patcher._patch_file", side_effect=Exception("Test error")
            ):
                patch_sql_files(["models/model_a.sql"])

                mock_log_warn.assert_called()
