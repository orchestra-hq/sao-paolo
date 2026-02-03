from datetime import datetime

import pytest
import yaml

from orchestra_dbt.models import Freshness, FreshnessConfig, MaterialisationNode
from src.orchestra_dbt.constants import ORCHESTRA_REUSED_NODE
from src.orchestra_dbt.patcher import (
    patch_file,
    patch_seed_properties,
    revert_patch_file,
)


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


class TestPatchSeedProperties:
    SEEDS_TO_REUSE = {
        "seed_1": MaterialisationNode(
            checksum="checksum_1",
            dbt_path="dbt_path_1",
            file_path="seeds/some_path/seed_1.csv",
            freshness_config=FreshnessConfig(),
            freshness=Freshness.CLEAN,
            sources={},
            last_updated=datetime(2025, 3, 4, 12, 30),
            reason="Seed seed_1 in same state as before.",
        ),
        "seed_2": MaterialisationNode(
            checksum="checksum_2",
            dbt_path="dbt_path_2",
            file_path="seed_2.csv",
            freshness_config=FreshnessConfig(),
            freshness=Freshness.CLEAN,
            sources={},
            last_updated=datetime(2025, 4, 5, 13, 45),
            reason="Seed seed_2 in same state as before.",
        ),
    }

    EXISTING_SEED_PROPERTIES = {
        "seeds": [
            {
                "name": "seed_1",
                "config": {"tags": ["tag_1"], "meta": {"customer_value": "one"}},
            },
        ]
    }

    def test_patch_existing_seed_properties_no_seeds_to_reuse(self, tmp_path):
        seeds_properties_file = tmp_path / "seeds" / "properties.yml"
        seeds_properties_file.parent.mkdir(parents=True, exist_ok=True)
        seeds_properties_file.write_text(
            yaml.safe_dump(self.EXISTING_SEED_PROPERTIES), encoding="utf-8"
        )
        patch_seed_properties(
            nodes_to_reuse={}, seed_properties_file_path=str(seeds_properties_file)
        )
        assert (
            yaml.safe_load(seeds_properties_file.read_text(encoding="utf-8"))
            == self.EXISTING_SEED_PROPERTIES
        )

    @pytest.mark.parametrize(
        "properties_file_exists, expected_seed_1_value",
        [
            (
                True,
                {
                    "name": "seed_1",
                    "config": {
                        "tags": ["tag_1", ORCHESTRA_REUSED_NODE],
                        "meta": {
                            "customer_value": "one",
                            "orchestra_reused_reason": "Seed seed_1 in same state as before.",
                            "orchestra_last_updated": "2025-03-04T12:30:00",
                        },
                    },
                },
            ),
            (
                False,
                {
                    "name": "seed_1",
                    "config": {
                        "tags": [ORCHESTRA_REUSED_NODE],
                        "meta": {
                            "orchestra_reused_reason": "Seed seed_1 in same state as before.",
                            "orchestra_last_updated": "2025-03-04T12:30:00",
                        },
                    },
                },
            ),
        ],
    )
    def test_patch_existing_seed_properties_with_seeds_to_reuse(
        self, tmp_path, properties_file_exists: bool, expected_seed_1_value: dict
    ):
        seeds_properties_file = tmp_path / "seeds" / "properties.yml"
        seeds_properties_file.parent.mkdir(parents=True, exist_ok=True)

        if properties_file_exists:
            seeds_properties_file.write_text(
                yaml.safe_dump(self.EXISTING_SEED_PROPERTIES), encoding="utf-8"
            )

        patch_seed_properties(
            nodes_to_reuse=self.SEEDS_TO_REUSE,
            seed_properties_file_path=str(seeds_properties_file),
        )

        assert yaml.safe_load(seeds_properties_file.read_text(encoding="utf-8")) == {
            "seeds": [
                expected_seed_1_value,
                {
                    "name": "seed_2",
                    "config": {
                        "tags": [ORCHESTRA_REUSED_NODE],
                        "meta": {
                            "orchestra_reused_reason": "Seed seed_2 in same state as before.",
                            "orchestra_last_updated": "2025-04-05T13:45:00",
                        },
                    },
                },
            ]
        }

    def test_invalid_seed_properties_file(self, tmp_path, capsys):
        seeds_properties_file = tmp_path / "seeds" / "properties.yml"
        seeds_properties_file.parent.mkdir(parents=True, exist_ok=True)
        seeds_properties_file.write_text("invalid yaml", encoding="utf-8")
        patch_seed_properties(
            nodes_to_reuse=self.SEEDS_TO_REUSE,
            seed_properties_file_path=str(seeds_properties_file),
        )
        assert seeds_properties_file.read_text(encoding="utf-8") == "invalid yaml"
        assert (
            "Missing 'seeds' key. Skipping patching of seeds."
            in capsys.readouterr().out
        )
