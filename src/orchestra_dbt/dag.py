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


def calculate_freshness_on_model(
    node_id: str, checksum: str, state: StateApiModel
) -> tuple[Freshness, str]:
    if node_id not in state.state:
        return Freshness.DIRTY, "Node not previously seen in state."
    if not checksum or checksum != state.state[node_id].checksum:
        return Freshness.DIRTY, "Checksum changed since last run."
    return Freshness.CLEAN, "Model in same state as last run."


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
        model_path = str(node["original_file_path"])
        if node["package_name"] != project_name_from_manifest:
            sql_path = f"dbt_packages/{node['package_name']}/{model_path}"
        else:
            sql_path = model_path

        freshness, reason = calculate_freshness_on_model(node_id, checksum, state)

        nodes[node_id] = ModelNode(
            freshness=freshness,
            checksum=checksum,
            freshness_config=node.get("config", {}).get("freshness"),
            last_updated=(
                state.state[node_id].last_updated if node_id in state.state else None
            ),
            model_path=model_path,
            sources=state.state[node_id].sources if node_id in state.state else {},
            sql_path=sql_path,
            reason=reason,
        )

        for dep in node.get("depends_on", {}).get("nodes", []):
            edges.append(Edge(from_=str(dep), to_=node_id))

    return ParsedDag(nodes=nodes, edges=edges)
