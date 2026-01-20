from datetime import datetime

import pytest

from src.orchestra_dbt.constants import ORCHESTRA_REUSED_NODE
from src.orchestra_dbt.patcher import patch_file, revert_patch_file


class TestPatchFile:
    def test_patch_file_without_existing_config(self, tmp_path):
        sql_file = tmp_path / "test_model.sql"
        original_content = "SELECT * FROM customers"
        sql_file.write_text(original_content, encoding="utf-8")

        patch_file(
            file_path=sql_file,
            reason="Test \"reason\" with 'quotes'.",
            freshness=None,
            last_updated=None,
        )

        expected_config = f"{{{{ config(tags=[\"{ORCHESTRA_REUSED_NODE}\"], meta={{'orchestra_reused_reason': 'Test reason with quotes.'}}) }}}}\n\n"
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

        patch_file(
            file_path=sql_file,
            reason="Test reason",
            freshness=34,
            last_updated=datetime(2025, 3, 4, 12, 30),
        )

        result = sql_file.read_text(encoding="utf-8")
        expected_config = f"{{{{ config(tags=[\"{ORCHESTRA_REUSED_NODE}\"], meta={{'orchestra_reused_reason': 'Test reason', 'orchestra_freshness': 34, 'orchestra_last_updated': '2025-03-04T12:30:00'}}) }}}}\n\n"

        assert result.startswith(expected_config)
        assert "-- This is a comment" in result
        assert "SELECT" in result
        assert "FROM customers" in result
        assert "old_tag" in result

    @pytest.mark.parametrize(
        "freshness, last_updated",
        [
            (None, None),
            (1, None),
            (None, datetime(2025, 3, 4, 12, 30)),
            (240, datetime(2025, 3, 4, 12, 30)),
        ],
    )
    def test_revert_patch_file(
        self, tmp_path, freshness: int | None, last_updated: datetime | None
    ):
        sql_file = tmp_path / "test_model.sql"
        original_content = "\n\tTEST_CONTENT\n"
        sql_file.write_text(original_content, encoding="utf-8")
        patch_file(
            file_path=sql_file,
            reason="Test reason",
            freshness=freshness,
            last_updated=last_updated,
        )
        revert_patch_file(file_path=sql_file)
        assert sql_file.read_text(encoding="utf-8") == original_content
