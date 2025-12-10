from src.orchestra_dbt.constants import ORCHESTRA_REUSED_NODE
from src.orchestra_dbt.patcher import patch_file


class TestPatchFile:
    def test_patch_file_without_existing_config(self, tmp_path):
        sql_file = tmp_path / "test_model.sql"
        original_content = "SELECT * FROM customers"
        sql_file.write_text(original_content, encoding="utf-8")

        patch_file(sql_file)

        expected_config = f'{{{{ config(tags=["{ORCHESTRA_REUSED_NODE}"]) }}}}\n\n'
        assert (
            sql_file.read_text(encoding="utf-8") == expected_config + original_content
        )

    def test_patch_file_preserves_content(self, tmp_path):
        sql_file = tmp_path / "test_model.sql"
        original_content = """-- This is a comment
{{ config(tags=["old_tag"]) }}
SELECT 
    id,
    name
FROM customers
WHERE active = true
"""
        sql_file.write_text(original_content, encoding="utf-8")

        patch_file(sql_file)

        result = sql_file.read_text(encoding="utf-8")
        expected_config = f'{{{{ config(tags=["{ORCHESTRA_REUSED_NODE}"]) }}}}\n\n'

        assert result.startswith(expected_config)
        assert "-- This is a comment" in result
        assert "SELECT" in result
        assert "FROM customers" in result
        assert "old_tag" in result
