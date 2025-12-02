from .models import (
    Edge,
    Freshness,
    Node,
    NodeType,
    ParsedDag,
    SourceFreshness,
    StateApiModel,
)
from .utils import load_file


def construct_dag(source_freshness: SourceFreshness, state: StateApiModel) -> ParsedDag:
    manifest = load_file("target/manifest.json")

    nodes: dict[str, Node] = {}
    edges: list[Edge] = []

    for node_id in manifest.get("child_map", {}).keys():
        node_id = str(node_id)
        if not node_id.startswith("source."):
            continue

        nodes[node_id] = Node(
            freshness=(
                Freshness.DIRTY
                if node_id not in state.state
                or source_freshness.sources[node_id] > state.state[node_id].last_updated
                else Freshness.CLEAN
            ),
            type=NodeType.SOURCE,
            last_updated=source_freshness.sources[node_id],
        )

    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue

        node_id = str(node_id)
        checksum = node.get("checksum", {}).get("checksum")
        checksum = str(checksum) if checksum else None

        nodes[node_id] = Node(
            freshness=(
                Freshness.DIRTY
                if node_id not in state.state
                or not checksum
                or checksum != state.state[node_id].checksum
                else Freshness.CLEAN
            ),
            type=NodeType.MODEL,
            checksum=checksum,
            freshness_config=node.get("config", {}).get("freshness"),
            last_updated=(
                state.state[node_id].last_updated if node_id in state.state else None
            ),
            sql_path=node.get("original_file_path"),
        )

        for dep in node.get("depends_on", {}).get("nodes", []):
            edges.append(Edge(from_=str(dep), to_=node_id))

    return ParsedDag(nodes=nodes, edges=edges)
