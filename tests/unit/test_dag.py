from datetime import datetime
from unittest.mock import patch

from src.orchestra_dbt.dag import construct_dag
from src.orchestra_dbt.models import (
    Edge,
    Freshness,
    Node,
    NodeType,
    ParsedDag,
    SourceFreshness,
    StateApiModel,
    StateItem,
)


class TestConstructDag:
    @patch("src.orchestra_dbt.dag.load_json")
    def test_construct_dag_with_sources(self, mock_load_json, sample_manifest):
        mock_load_json.return_value = sample_manifest

        source_freshness = SourceFreshness(
            sources={
                "source.test_db.test_schema.test_table": datetime(2024, 1, 3, 12, 0, 0),
            }
        )
        state = StateApiModel(
            state={
                "model.test_project.model_a": StateItem(
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="def456",
                    sources={},
                )
            }
        )

        assert construct_dag(source_freshness, state) == ParsedDag(
            nodes={
                "source.test_db.test_schema.test_table": Node(
                    freshness=Freshness.CLEAN,
                    type=NodeType.SOURCE,
                    last_updated=datetime(2024, 1, 3, 12, 0, 0),
                ),
                "model.test_project.model_a": Node(
                    freshness=Freshness.CLEAN,
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    type=NodeType.MODEL,
                    checksum="def456",
                    sql_path="models/model_a.sql",
                ),
                "model.test_project.model_b": Node(
                    freshness=Freshness.DIRTY,
                    type=NodeType.MODEL,
                    checksum="ghi789",
                    sql_path="models/model_b.sql",
                ),
            },
            edges=[
                Edge(
                    from_="source.test_db.test_schema.test_table",
                    to_="model.test_project.model_a",
                ),
                Edge(
                    from_="model.test_project.model_a",
                    to_="model.test_project.model_b",
                ),
            ],
        )

    @patch("src.orchestra_dbt.dag.load_json")
    def test_construct_dag_with_models(self, mock_load_json, sample_manifest):
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
                    checksum="def456",
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
        # Model should be CLEAN since checksum matches
        assert dag.nodes["model.test_project.model_a"].freshness == Freshness.CLEAN
        assert dag.nodes["model.test_project.model_a"].type == NodeType.MODEL

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
        assert dag.nodes["model.test_project.model_a"].freshness == Freshness.DIRTY
