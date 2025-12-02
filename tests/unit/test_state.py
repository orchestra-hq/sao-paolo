from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

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
from src.orchestra_dbt.state import (
    build_sla_duration,
    calculate_models_to_run,
    construct_dag,
    get_source_freshness,
)


class TestGetSourceFreshness:
    @patch("src.orchestra_dbt.state.source_freshness_invoke")
    @patch("src.orchestra_dbt.state.load_file")
    def test_get_source_freshness_success(
        self, mock_load_file, mock_invoke, sample_sources_json
    ):
        mock_load_file.return_value = sample_sources_json
        assert get_source_freshness() == SourceFreshness(
            sources={
                "source.test_db.test_schema.test_table": datetime(2024, 1, 3, 12, 0, 0),
            }
        )
        mock_invoke.assert_called_once()


class TestConstructDag:
    @patch("src.orchestra_dbt.state.load_file")
    def test_construct_dag_with_sources(self, mock_load_file, sample_manifest):
        mock_load_file.return_value = sample_manifest

        source_freshness = SourceFreshness(
            sources={
                "source.test_db.test_schema.test_table": datetime(2024, 1, 3, 12, 0, 0),
            }
        )
        state = StateApiModel(
            state={
                "source.test_db.test_schema.test_table": StateItem(
                    last_updated=datetime(2024, 1, 1, 12, 0, 0),
                    checksum=None,
                )
            }
        )

        assert construct_dag(source_freshness, state) == ParsedDag(
            nodes={
                "source.test_db.test_schema.test_table": Node(
                    freshness=Freshness.DIRTY,
                    type=NodeType.SOURCE,
                    last_updated=datetime(2024, 1, 3, 12, 0, 0),
                ),
                "model.test_project.model_a": Node(
                    freshness=Freshness.DIRTY,
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

    @patch("src.orchestra_dbt.state.load_file")
    def test_construct_dag_with_models(self, mock_load_file, sample_manifest):
        mock_load_file.return_value = sample_manifest

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
                )
            }
        )

        dag = construct_dag(source_freshness, state)

        assert "model.test_project.model_a" in dag.nodes
        # Model should be CLEAN since checksum matches
        assert dag.nodes["model.test_project.model_a"].freshness == Freshness.CLEAN
        assert dag.nodes["model.test_project.model_a"].type == NodeType.MODEL

    @patch("src.orchestra_dbt.state.load_file")
    def test_construct_dag_dirty_model(self, mock_load_file, sample_manifest):
        mock_load_file.return_value = sample_manifest

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
                )
            }
        )

        dag = construct_dag(source_freshness, state)

        assert "model.test_project.model_a" in dag.nodes
        # Model should be DIRTY since checksum doesn't match
        assert dag.nodes["model.test_project.model_a"].freshness == Freshness.DIRTY


class TestBuildSlaDuration:
    @pytest.mark.parametrize(
        "period, count, expected",
        [("minute", 30, 30), ("hour", 2, 120), ("day", 1, 1440)],
    )
    def test_build_sla_duration_minutes(self, period: str, count: int, expected: int):
        assert build_sla_duration({"period": period, "count": count}) == expected

    def test_build_sla_duration_invalid_period(self):
        with pytest.raises(ValueError, match="Invalid period"):
            build_sla_duration({"period": "invalid", "count": 1})

    def test_build_sla_duration_invalid_count(self):
        with pytest.raises(ValueError, match="Invalid count"):
            build_sla_duration({"period": "minute", "count": "invalid"})


class TestCalculateModelsToRun:
    def test_calculate_models_to_run_propagates_dirty(self):
        # Create a simple DAG: source -> model_a -> model_b
        nodes = {
            "source.test": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.SOURCE,
                last_updated=datetime.now(),
            ),
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
            "model.b": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
        }
        edges = [
            Edge(from_="source.test", to_="model.a"),
            Edge(from_="model.a", to_="model.b"),
        ]

        result = calculate_models_to_run(
            ParsedDag(nodes=nodes, edges=edges), StateApiModel(state={})
        )

        # Both models should be dirty now
        assert result.nodes["model.a"].freshness == Freshness.DIRTY
        assert result.nodes["model.b"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_preserves_clean(self):
        nodes = {
            "source.test": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.SOURCE,
                last_updated=datetime.now(),
            ),
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config=None,
            ),
        }
        edges = [Edge(from_="source.test", to_="model.a")]

        result = calculate_models_to_run(
            ParsedDag(nodes=nodes, edges=edges), StateApiModel(state={})
        )

        # Model should remain clean
        assert result.nodes["model.a"].freshness == Freshness.CLEAN

    def test_calculate_models_to_run_respects_sla(self):
        recent_time = datetime.now() - timedelta(minutes=30)
        nodes = {
            "source.test": Node(
                freshness=Freshness.DIRTY,
                type=NodeType.SOURCE,
                last_updated=datetime.now(),
            ),
            "model.a": Node(
                freshness=Freshness.CLEAN,
                type=NodeType.MODEL,
                freshness_config={"build_after": {"count": 1, "period": "hour"}},
            ),
        }
        edges = [Edge(from_="source.test", to_="model.a")]

        result = calculate_models_to_run(
            ParsedDag(nodes=nodes, edges=edges),
            StateApiModel(
                state={
                    "model.a": StateItem(
                        last_updated=recent_time,
                        checksum="abc123",
                    )
                }
            ),
        )

        # Model should remain clean due to SLA
        assert result.nodes["model.a"].freshness == Freshness.CLEAN
