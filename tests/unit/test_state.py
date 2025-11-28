from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from src.orchestra_dbt.models import (
    Freshness,
    NodeType,
    SourceFreshness,
    StateApiModel,
    StateItem,
)
from src.orchestra_dbt.state import (
    _valid_sla,
    build_sla_duration,
    calculate_models_to_run,
    construct_dag,
    get_source_freshness,
)


class TestGetSourceFreshness:
    """Tests for get_source_freshness function."""

    @patch("orchestra_dbt.state.source_freshness_invoke")
    @patch("orchestra_dbt.state.load_file")
    def test_get_source_freshness_success(self, mock_load_file, mock_invoke):
        """Test successful source freshness retrieval."""
        mock_load_file.return_value = {
            "results": [
                {
                    "unique_id": "source.test_db.test_schema.test_table",
                    "max_loaded_at": datetime(2024, 1, 3, 12, 0, 0),
                }
            ]
        }

        result = get_source_freshness()

        assert isinstance(result, SourceFreshness)
        assert "source.test_db.test_schema.test_table" in result.sources
        mock_invoke.assert_called_once()
        mock_load_file.assert_called_once_with("target/sources.json")


class TestConstructDag:
    """Tests for construct_dag function."""

    @patch("orchestra_dbt.state.load_file")
    def test_construct_dag_with_sources(self, mock_load_file, sample_manifest):
        """Test DAG construction with sources."""
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

        dag = construct_dag(source_freshness, state)

        assert len(dag.nodes) > 0
        assert len(dag.edges) > 0
        assert "source.test_db.test_schema.test_table" in dag.nodes
        # Source should be DIRTY since freshness is newer than state
        assert (
            dag.nodes["source.test_db.test_schema.test_table"].freshness
            == Freshness.DIRTY
        )

    @patch("orchestra_dbt.state.load_file")
    def test_construct_dag_with_models(self, mock_load_file, sample_manifest):
        """Test DAG construction with models."""
        mock_load_file.return_value = sample_manifest

        source_freshness = SourceFreshness(sources={})
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

    @patch("orchestra_dbt.state.load_file")
    def test_construct_dag_dirty_model(self, mock_load_file, sample_manifest):
        """Test DAG construction with dirty model (checksum mismatch)."""
        mock_load_file.return_value = sample_manifest

        source_freshness = SourceFreshness(sources={})
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
    """Tests for build_sla_duration function."""

    def test_build_sla_duration_minutes(self):
        """Test SLA duration calculation in minutes."""
        build_after = {"period": "minute", "count": 30}
        result = build_sla_duration(build_after)
        assert result == 30

    def test_build_sla_duration_hours(self):
        """Test SLA duration calculation in hours."""
        build_after = {"period": "hour", "count": 2}
        result = build_sla_duration(build_after)
        assert result == 120

    def test_build_sla_duration_days(self):
        """Test SLA duration calculation in days."""
        build_after = {"period": "day", "count": 1}
        result = build_sla_duration(build_after)
        assert result == 1440

    def test_build_sla_duration_invalid_period(self):
        """Test that invalid period raises ValueError."""
        build_after = {"period": "invalid", "count": 1}
        with pytest.raises(ValueError, match="Invalid period"):
            build_sla_duration(build_after)


class TestValidSla:
    """Tests for _valid_sla function."""

    def test_valid_sla_no_config(self):
        """Test that SLA is valid when no config is provided."""
        state = StateApiModel(state={})
        result = _valid_sla("model.test", None, state)
        assert result is True

    def test_valid_sla_no_build_after(self):
        """Test that SLA is valid when build_after is not in config."""
        state = StateApiModel(state={})
        config = {"warn_after": {"count": 1, "period": "hour"}}
        result = _valid_sla("model.test", config, state)
        assert result is True

    def test_valid_sla_no_state(self):
        """Test that SLA is valid when model is not in state."""
        state = StateApiModel(state={})
        config = {"build_after": {"count": 1, "period": "hour"}}
        result = _valid_sla("model.test", config, state)
        assert result is True

    def test_valid_sla_expired(self):
        """Test that SLA is valid when build_after time has passed."""
        past_time = datetime.now() - timedelta(hours=2)
        state = StateApiModel(
            state={
                "model.test": StateItem(
                    last_updated=past_time,
                    checksum="abc123",
                )
            }
        )
        config = {"build_after": {"count": 1, "period": "hour"}}
        result = _valid_sla("model.test", config, state)
        assert result is True

    def test_valid_sla_not_expired(self):
        """Test that SLA is invalid when build_after time has not passed."""
        recent_time = datetime.now() - timedelta(minutes=30)
        state = StateApiModel(
            state={
                "model.test": StateItem(
                    last_updated=recent_time,
                    checksum="abc123",
                )
            }
        )
        config = {"build_after": {"count": 1, "period": "hour"}}
        result = _valid_sla("model.test", config, state)
        assert result is False


class TestCalculateModelsToRun:
    """Tests for calculate_models_to_run function."""

    def test_calculate_models_to_run_propagates_dirty(self):
        """Test that dirty status propagates to downstream models."""
        from src.orchestra_dbt.models import Edge, Node, ParsedDag

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
        dag = ParsedDag(nodes=nodes, edges=edges)
        state = StateApiModel(state={})

        result = calculate_models_to_run(dag, state)

        # Both models should be dirty now
        assert result.nodes["model.a"].freshness == Freshness.DIRTY
        assert result.nodes["model.b"].freshness == Freshness.DIRTY

    def test_calculate_models_to_run_preserves_clean(self):
        """Test that clean sources keep downstream models clean."""
        from src.orchestra_dbt.models import Edge, Node, ParsedDag

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
        dag = ParsedDag(nodes=nodes, edges=edges)
        state = StateApiModel(state={})

        result = calculate_models_to_run(dag, state)

        # Model should remain clean
        assert result.nodes["model.a"].freshness == Freshness.CLEAN

    def test_calculate_models_to_run_respects_sla(self):
        """Test that SLA restrictions are respected."""
        from src.orchestra_dbt.models import Edge, Node, ParsedDag

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
        dag = ParsedDag(nodes=nodes, edges=edges)
        state = StateApiModel(
            state={
                "model.a": StateItem(
                    last_updated=recent_time,
                    checksum="abc123",
                )
            }
        )

        result = calculate_models_to_run(dag, state)

        # Model should remain clean due to SLA
        assert result.nodes["model.a"].freshness == Freshness.CLEAN
