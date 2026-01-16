from datetime import datetime
from unittest.mock import patch

from src.orchestra_dbt.dag import calculate_freshness_on_node, construct_dag
from src.orchestra_dbt.models import (
    Edge,
    Freshness,
    MaterialisationNode,
    ParsedDag,
    SourceFreshness,
    SourceNode,
    StateApiModel,
    StateItem,
)


class TestCalculateFreshnessOnNode:
    def test_calculate_freshness_on_node_snapshot(self):
        assert calculate_freshness_on_node(
            asset_external_id="test.snapshot.a.b",
            checksum="123",
            state=StateApiModel(state={}),
            resource_type="snapshot",
            track_state=True,
        ) == (Freshness.DIRTY, "Snapshot is always dirty.")

    def test_calculate_freshness_on_node_not_tracking_state(self):
        assert calculate_freshness_on_node(
            asset_external_id="test.seed.a.b",
            checksum="123",
            state=StateApiModel(state={}),
            resource_type="seed",
            track_state=False,
        ) == (Freshness.DIRTY, "State orchestration for this node is disabled.")

    def test_calculate_freshness_on_node_not_in_state(self):
        assert calculate_freshness_on_node(
            asset_external_id="test.model.a.b",
            checksum="123",
            state=StateApiModel(state={}),
            resource_type="model",
            track_state=True,
        ) == (Freshness.DIRTY, "Model not previously seen in state.")

    def test_calculate_freshness_on_node_checksum_changed(self):
        assert calculate_freshness_on_node(
            asset_external_id="test.model.a.b",
            checksum="123",
            state=StateApiModel(
                state={
                    "test.model.a.b": StateItem(
                        last_updated=datetime(2024, 1, 1, 12, 0, 0),
                        checksum="456",
                        sources={},
                    )
                }
            ),
            resource_type="model",
            track_state=True,
        ) == (Freshness.DIRTY, "Checksum changed since last run.")

    def test_calculate_freshness_on_node_checksum_matches(self):
        assert calculate_freshness_on_node(
            asset_external_id="test.model.a.b",
            checksum="123",
            state=StateApiModel(
                state={
                    "test.model.a.b": StateItem(
                        last_updated=datetime(2024, 1, 1, 12, 0, 0),
                        checksum="123",
                        sources={},
                    ),
                }
            ),
            resource_type="model",
            track_state=True,
        ) == (Freshness.CLEAN, "Model in same state as last run.")


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
                "model.test_project.model_a": MaterialisationNode(
                    freshness=Freshness.CLEAN,
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="def456",
                    node_path="models/model_a.sql",
                    sql_path="models/model_a.sql",
                    sources={
                        "source.test_db.test_schema.test_table": datetime(
                            2024, 1, 3, 12, 0, 0
                        ),
                    },
                    reason="Model in same state as last run.",
                ),
                "model.test_project_2.model_b": MaterialisationNode(
                    freshness=Freshness.DIRTY,
                    checksum="ghi789",
                    node_path="models/model_b.sql",
                    sql_path="dbt_packages/test_project_2/models/model_b.sql",
                    reason="Model not previously seen in state.",
                    sources={},
                ),
                "model.test_project.model_c": MaterialisationNode(
                    freshness=Freshness.DIRTY,
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum="456",
                    node_path="models/model_c.sql",
                    sql_path="models/model_c.sql",
                    reason="Checksum changed since last run.",
                    sources={},
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
        assert isinstance(dag.nodes["model.test_project.model_a"], MaterialisationNode)
        assert dag.nodes["model.test_project.model_a"].freshness == Freshness.DIRTY
