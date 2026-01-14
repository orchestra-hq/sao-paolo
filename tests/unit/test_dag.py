from datetime import datetime
from unittest.mock import patch

from src.orchestra_dbt.dag import construct_dag
from src.orchestra_dbt.models import (
    Edge,
    Freshness,
    ModelNode,
    ParsedDag,
    SourceFreshness,
    SourceNode,
    StateApiModel,
    StateItem,
)


class TestConstructDag:
    @patch("src.orchestra_dbt.dag.load_json")
    @patch("src.orchestra_dbt.dag.get_integration_account_id_from_env")
    def test_construct_dag_with_sources(
        self, mock_get_integration_account_id_from_env, mock_load_json, sample_manifest
    ):
        mock_get_integration_account_id_from_env.return_value = "integration_account_id"
        mock_load_json.return_value = sample_manifest

        source_freshness = SourceFreshness(
            sources={
                "source.test_db.test_schema.test_table": datetime(2024, 1, 3, 12, 0, 0),
            }
        )
        state = StateApiModel(
            state={
                "integration_account_id.model.test_project.model_a": StateItem(
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="def456",
                    sources={
                        "source.test_db.test_schema.test_table": datetime(
                            2024, 1, 3, 12, 0, 0
                        ),
                    },
                ),
                "integration_account_id.model.test_project.model_c": StateItem(
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="123",
                    sources={},
                ),
                "other_integration_account_id.model.test_project.model_a": StateItem(
                    last_updated=datetime(2023, 1, 1, 12, 0, 0),
                    checksum="other_checksum",
                    sources={},
                ),
            }
        )

        assert construct_dag(source_freshness, state) == ParsedDag(
            nodes={
                "source.test_db.test_schema.test_table": SourceNode(
                    last_updated=datetime(2024, 1, 3, 12, 0, 0),
                ),
                "model.test_project.model_a": ModelNode(
                    freshness=Freshness.CLEAN,
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="def456",
                    model_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    sources={
                        "source.test_db.test_schema.test_table": datetime(
                            2024, 1, 3, 12, 0, 0
                        ),
                    },
                    reason="Model in same state as last run.",
                ),
                "model.test_project_2.model_b": ModelNode(
                    freshness=Freshness.DIRTY,
                    checksum="ghi789",
                    model_path="models/model_b.sql",
                    sql_path="dbt_packages/test_project_2/models/model_b.sql",
                    reason="Node not previously seen in state.",
                ),
                "model.test_project.model_c": ModelNode(
                    freshness=Freshness.DIRTY,
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="456",
                    model_path="models/model_c.sql",
                    sql_path="models/model_c.sql",
                    reason="Checksum changed since last run.",
                ),
            },
            edges=[
                Edge(
                    from_="source.test_db.test_schema.test_table",
                    to_="model.test_project.model_a",
                ),
                Edge(
                    from_="model.test_project.model_a",
                    to_="model.test_project_2.model_b",
                ),
            ],
        )

    @patch("src.orchestra_dbt.dag.load_json")
    def test_construct_dag_dirty_model(self, mock_load_json, sample_manifest):
        mock_load_json.return_value = sample_manifest

        source_freshness = SourceFreshness(
            sources={
                "source.test_db.test_schema.test_table": datetime(2024, 1, 3, 12, 0, 0),
            }
        )
        state = StateApiModel(
            state={
                "model.test_project.model_a": StateItem(
                    last_updated=datetime(2024, 1, 2, 12, 0, 0),
                    checksum="old_checksum",  # Different from manifest
                    sources={
                        "source.test_db.test_schema.test_table": datetime(
                            2024, 1, 3, 12, 0, 0
                        ),
                    },
                )
            }
        )

        dag = construct_dag(source_freshness, state)

        assert "model.test_project.model_a" in dag.nodes
        # Model should be DIRTY since checksum doesn't match
        assert isinstance(dag.nodes["model.test_project.model_a"], ModelNode)
        assert dag.nodes["model.test_project.model_a"].freshness == Freshness.DIRTY
