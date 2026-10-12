from datetime import UTC, datetime
from typing import Any, ClassVar

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
            last_updated=datetime(2025, 3, 4, 12, 30, tzinfo=UTC),
        )

        result = sql_file.read_text(encoding="utf-8")
        expected_config = f"{{{{ config(tags=[\"{ORCHESTRA_REUSED_NODE}\"], meta={{'orchestra_reused_reason': 'Test reason', 'orchestra_freshness': 34, 'orchestra_last_updated': '2025-03-04T12:30:00+00:00'}}) }}}}\n\n"

        assert result.startswith(expected_config)
        assert "-- This is a comment" in result
        assert "SELECT" in result
        assert "FROM customers" in result
        assert "old_tag" in result

    def test_patch_file_keeps_zero_freshness(self, tmp_path) -> None:
        sql_file = tmp_path / "test_model.sql"
        sql_file.write_text("select 1", encoding="utf-8")

        patch_file(
            file_path=sql_file,
            reason="Test reason",
            freshness=0,
            last_updated=None,
        )

        result = sql_file.read_text(encoding="utf-8")
        assert "'orchestra_freshness': 0" in result

    @pytest.mark.parametrize(
        "freshness, last_updated",
        [
            (None, None),
            (1, None),
            (None, datetime(2025, 3, 4, 12, 30, tzinfo=UTC)),
            (240, datetime(2025, 3, 4, 12, 30, tzinfo=UTC)),
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
    SEEDS_TO_REUSE: ClassVar[dict[str, MaterialisationNode]] = {
        "seed_1": MaterialisationNode(
            asset_external_id="integration_account_id.seed_1",
            checksum="checksum_1",
            dbt_path="seeds/seed_1.csv",
            file_path="seeds/seed_1.csv",
            freshness_config=FreshnessConfig(),
            freshness=Freshness.CLEAN,
            sources={},
            last_updated=datetime(2025, 3, 4, 12, 30, tzinfo=UTC),
            reason="Seed seed_1 in same state as before.",
        ),
        "seed_2": MaterialisationNode(
            asset_external_id="integration_account_id.seed_2",
            checksum="checksum_2",
            dbt_path="seeds/seed_2.csv",
            file_path="seeds/seed_2.csv",
            freshness_config=FreshnessConfig(),
            freshness=Freshness.CLEAN,
            sources={},
            last_updated=datetime(2025, 4, 5, 13, 45, tzinfo=UTC),
            reason="Seed seed_2 in same state as before.",
        ),
    }

    EXISTING_SEED_PROPERTIES: ClassVar[dict[str, Any]] = {
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
        patch_seed_properties(nodes_to_reuse={}, project_dir=tmp_path)
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
                            "orchestra_last_updated": "2025-03-04T12:30:00+00:00",
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
                            "orchestra_last_updated": "2025-03-04T12:30:00+00:00",
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
            project_dir=tmp_path,
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
                            "orchestra_last_updated": "2025-04-05T13:45:00+00:00",
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
            project_dir=tmp_path,
        )
        assert seeds_properties_file.read_text(encoding="utf-8") == "invalid yaml"
        assert (
            "Missing 'seeds' key. Skipping patching of seeds."
            in capsys.readouterr().out
        )

    def test_patch_seed_properties_creates_config_when_missing(self, tmp_path) -> None:
        seeds_properties_file = tmp_path / "seeds" / "properties.yml"
        seeds_properties_file.parent.mkdir(parents=True, exist_ok=True)
        seeds_properties_file.write_text(
            yaml.safe_dump({"seeds": [{"name": "seed_1"}]}), encoding="utf-8"
        )

        patch_seed_properties(
            nodes_to_reuse=self.SEEDS_TO_REUSE,
            project_dir=tmp_path,
        )

        saved = yaml.safe_load(seeds_properties_file.read_text(encoding="utf-8"))
        seed_1 = next(seed for seed in saved["seeds"] if seed["name"] == "seed_1")
        assert seed_1["config"]["tags"] == [ORCHESTRA_REUSED_NODE]
        assert seed_1["config"]["meta"]["orchestra_reused_reason"] == (
            "Seed seed_1 in same state as before."
        )

    def test_patch_seed_properties_does_not_duplicate_reused_tag(
        self, tmp_path
    ) -> None:
        seeds_properties_file = tmp_path / "seeds" / "properties.yml"
        seeds_properties_file.parent.mkdir(parents=True, exist_ok=True)
        seeds_properties_file.write_text(
            yaml.safe_dump(
                {
                    "seeds": [
                        {
                            "name": "seed_1",
                            "config": {"tags": [ORCHESTRA_REUSED_NODE], "meta": {}},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        patch_seed_properties(
            nodes_to_reuse=self.SEEDS_TO_REUSE,
            project_dir=tmp_path,
        )

        saved = yaml.safe_load(seeds_properties_file.read_text(encoding="utf-8"))
        seed_1 = next(seed for seed in saved["seeds"] if seed["name"] == "seed_1")
        assert seed_1["config"]["tags"].count(ORCHESTRA_REUSED_NODE) == 1

    def test_patch_seed_properties_handles_invalid_config_shape(
        self, tmp_path, capsys
    ) -> None:
        seeds_properties_file = tmp_path / "seeds" / "properties.yml"
        seeds_properties_file.parent.mkdir(parents=True, exist_ok=True)
        seeds_properties_file.write_text(
            yaml.safe_dump({"seeds": [{"name": "seed_1", "config": "invalid"}]}),
            encoding="utf-8",
        )

        patch_seed_properties(
            nodes_to_reuse=self.SEEDS_TO_REUSE,
            project_dir=tmp_path,
        )

        output = capsys.readouterr().out
        assert "Unknown error updating seed 'seed_1'" in output
        assert "'config' key must be a dictionary." in output

    def test_patches_the_seeds_own_properties_file(self, tmp_path) -> None:
        schema_file = tmp_path / "data" / "schema.yml"
        schema_file.parent.mkdir()
        original = yaml.safe_dump(
            {"seeds": [{"name": "seed_1", "description": "Seed one."}]}
        )
        schema_file.write_text(original, encoding="utf-8")
        seed_1 = self.SEEDS_TO_REUSE["seed_1"].model_copy(
            update={
                "dbt_path": "data/seed_1.csv",
                "file_path": "data/seed_1.csv",
                "patch_path": "my_project://data/schema.yml",
            }
        )

        snapshots = patch_seed_properties({"seed_1": seed_1}, project_dir=tmp_path)

        saved = yaml.safe_load(schema_file.read_text(encoding="utf-8"))
        assert [seed["name"] for seed in saved["seeds"]] == ["seed_1"]
        assert saved["seeds"][0]["config"]["tags"] == [ORCHESTRA_REUSED_NODE]
        assert not (tmp_path / "data" / "properties.yml").exists()
        assert snapshots == {schema_file: original.encode()}

    def test_resolves_patch_path_inside_a_package(self, tmp_path) -> None:
        schema_file = tmp_path / "dbt_packages" / "pkg" / "seeds" / "schema.yml"
        schema_file.parent.mkdir(parents=True)
        schema_file.write_text(
            yaml.safe_dump({"seeds": [{"name": "seed_1"}]}), encoding="utf-8"
        )
        seed_1 = self.SEEDS_TO_REUSE["seed_1"].model_copy(
            update={
                "file_path": "dbt_packages/pkg/seeds/seed_1.csv",
                "patch_path": "pkg://seeds/schema.yml",
            }
        )

        patch_seed_properties({"seed_1": seed_1}, project_dir=tmp_path)

        saved = yaml.safe_load(schema_file.read_text(encoding="utf-8"))
        assert saved["seeds"][0]["config"]["tags"] == [ORCHESTRA_REUSED_NODE]

    def test_writes_next_to_the_csv_without_a_properties_file(self, tmp_path) -> None:
        seed_1 = self.SEEDS_TO_REUSE["seed_1"].model_copy(
            update={"dbt_path": "data/seed_1.csv", "file_path": "data/seed_1.csv"}
        )

        snapshots = patch_seed_properties({"seed_1": seed_1}, project_dir=tmp_path)

        properties_file = tmp_path / "data" / "properties.yml"
        saved = yaml.safe_load(properties_file.read_text(encoding="utf-8"))
        assert saved["seeds"][0]["name"] == "seed_1"
        assert not (tmp_path / "seeds").exists()
        assert snapshots == {properties_file: None}
