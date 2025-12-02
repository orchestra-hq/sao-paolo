from datetime import datetime
from unittest.mock import patch

from src.orchestra_dbt.models import SourceFreshness
from src.orchestra_dbt.source_freshness import get_source_freshness


class TestGetSourceFreshness:
    @patch("src.orchestra_dbt.source_freshness.dbtRunner.invoke")
    @patch("src.orchestra_dbt.source_freshness.load_file")
    def test_get_source_freshness_success(
        self, mock_load_file, mock_invoke, sample_sources_json
    ):
        mock_load_file.return_value = sample_sources_json
        result = get_source_freshness()
        assert result is not None
        expected = SourceFreshness(
            sources={
                "source.test_db.test_schema.test_table": datetime.fromisoformat(
                    "2024-01-03T12:00:00"
                ),
            }
        )
        assert result.model_dump() == expected.model_dump()
        mock_invoke.assert_called_once()
