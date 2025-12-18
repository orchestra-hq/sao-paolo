from .models import (
    Edge,
    Freshness,
    ModelNode,
    Node,
    ParsedDag,
    SourceFreshness,
    SourceNode,
    StateApiModel,
)
from .utils import load_json


def construct_dag(source_freshness: SourceFreshness, state: StateApiModel) -> ParsedDag:
    manifest = load_json("target/manifest.json")

    nodes: dict[str, Node] = {}
    edges: list[Edge] = []

    project_name_from_manifest = manifest["metadata"]["project_name"]

    for node_id in manifest.get("child_map", {}).keys():
        node_id = str(node_id)
        if not node_id.startswith("source."):
            continue
        nodes[node_id] = SourceNode(last_updated=source_freshness.sources.get(node_id))

    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue

        node_id = str(node_id)
        checksum = str(node["checksum"]["checksum"])
        sql_path = str(node["original_file_path"])
        if node["package_name"] != project_name_from_manifest:
            sql_path = f"dbt_packages/{node['package_name']}/{sql_path}"

        nodes[node_id] = ModelNode(
            freshness=(
                Freshness.DIRTY
                if node_id not in state.state
                or not checksum
                or checksum != state.state[node_id].checksum
                else Freshness.CLEAN
            ),
            checksum=checksum,
            freshness_config=node.get("config", {}).get("freshness"),
            last_updated=(
                state.state[node_id].last_updated if node_id in state.state else None
            ),
            sources=state.state[node_id].sources if node_id in state.state else {},
            sql_path=sql_path,
        )

        for dep in node.get("depends_on", {}).get("nodes", []):
            edges.append(Edge(from_=str(dep), to_=node_id))

    return ParsedDag(nodes=nodes, edges=edges)
