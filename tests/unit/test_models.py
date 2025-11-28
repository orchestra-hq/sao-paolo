from datetime import datetime

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


class TestEnums:
    """Tests for enum types."""

    def test_freshness_enum(self):
        """Test Freshness enum values."""
        assert Freshness.CLEAN.value == "CLEAN"
        assert Freshness.DIRTY.value == "DIRTY"

    def test_node_type_enum(self):
        """Test NodeType enum values."""
        assert NodeType.MODEL.value == "MODEL"
        assert NodeType.SOURCE.value == "SOURCE"


class TestStateItem:
    """Tests for StateItem model."""

    def test_state_item_creation(self):
        """Test creating a StateItem."""
        item = StateItem(
            last_updated=datetime(2024, 1, 1, 12, 0, 0),
            checksum="abc123",
        )
        assert item.last_updated == datetime(2024, 1, 1, 12, 0, 0)
        assert item.checksum == "abc123"

    def test_state_item_optional_checksum(self):
        """Test StateItem with optional checksum."""
        item = StateItem(last_updated=datetime(2024, 1, 1, 12, 0, 0))
        assert item.checksum is None


class TestStateApiModel:
    """Tests for StateApiModel."""

    def test_state_api_model_creation(self):
        """Test creating a StateApiModel."""
        state = StateApiModel(state={})
        assert isinstance(state.state, dict)

    def test_state_api_model_with_items(self):
        """Test StateApiModel with state items."""
        state_items = {
            "model.test": StateItem(
                last_updated=datetime(2024, 1, 1, 12, 0, 0),
                checksum="abc123",
            )
        }
        state = StateApiModel(state=state_items)
        assert len(state.state) == 1
        assert "model.test" in state.state


class TestSourceFreshness:
    """Tests for SourceFreshness model."""

    def test_source_freshness_creation(self):
        """Test creating a SourceFreshness."""
        sources = {
            "source.test_db.test_schema.test_table": datetime(2024, 1, 1, 12, 0, 0)
        }
        freshness = SourceFreshness(sources=sources)
        assert len(freshness.sources) == 1


class TestNode:
    """Tests for Node model."""

    def test_node_creation_minimal(self):
        """Test creating a Node with minimal fields."""
        node = Node(freshness=Freshness.CLEAN, type=NodeType.MODEL)
        assert node.freshness == Freshness.CLEAN
        assert node.type == NodeType.MODEL
        assert node.checksum is None
        assert node.last_updated is None

    def test_node_creation_full(self):
        """Test creating a Node with all fields."""
        node = Node(
            freshness=Freshness.DIRTY,
            type=NodeType.MODEL,
            checksum="abc123",
            last_updated=datetime(2024, 1, 1, 12, 0, 0),
            freshness_config={"build_after": {"count": 1, "period": "hour"}},
            sql_path="models/model.sql",
        )
        assert node.checksum == "abc123"
        assert node.sql_path == "models/model.sql"
        assert node.freshness_config is not None


class TestEdge:
    """Tests for Edge model."""

    def test_edge_creation(self):
        """Test creating an Edge."""
        edge = Edge(from_="source.test", to_="model.test")
        assert edge.from_ == "source.test"
        assert edge.to_ == "model.test"


class TestParsedDag:
    """Tests for ParsedDag model."""

    def test_parsed_dag_creation(self):
        """Test creating a ParsedDag."""
        nodes = {"model.test": Node(freshness=Freshness.CLEAN, type=NodeType.MODEL)}
        edges = []
        dag = ParsedDag(nodes=nodes, edges=edges)
        assert len(dag.nodes) == 1
        assert len(dag.edges) == 0

    def test_parsed_dag_with_edges(self):
        """Test ParsedDag with edges."""
        nodes = {
            "source.test": Node(freshness=Freshness.CLEAN, type=NodeType.SOURCE),
            "model.test": Node(freshness=Freshness.CLEAN, type=NodeType.MODEL),
        }
        edges = [Edge(from_="source.test", to_="model.test")]
        dag = ParsedDag(nodes=nodes, edges=edges)
        assert len(dag.edges) == 1
        assert dag.edges[0].from_ == "source.test"
        assert dag.edges[0].to_ == "model.test"
