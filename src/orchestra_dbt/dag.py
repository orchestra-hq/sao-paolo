from .build_after import parse_freshness_config
from .checksum import calculate_checksum
from .models import (
    Edge,
    Freshness,
    MaterialisationNode,
    Node,
    ParsedDag,
    SourceFreshness,
    SourceNode,
    StateApiModel,
)
from .utils import get_integration_account_id_from_env, load_json


def calculate_freshness_on_node(
    asset_external_id: str,
    checksum: str,
    state: StateApiModel,
    resource_type: str,
    track_state: bool,
) -> tuple[Freshness, str]:
    if resource_type == "snapshot":
        # Note: currently, we always run snapshots. Need to configure how to propagate
        # the ability not to run snapshots via tags/meta.
        return Freshness.DIRTY, "Snapshot is always dirty."

    if not track_state:
        return Freshness.DIRTY, "State orchestration for this node is disabled."

    if asset_external_id not in state.state:
        return (
            Freshness.DIRTY,
            f"{resource_type.capitalize()} not previously seen in state.",
        )
    if checksum != state.state[asset_external_id].checksum:
        return Freshness.DIRTY, "Checksum changed since last run."
    return Freshness.CLEAN, f"{resource_type.capitalize()} in same state as last run."


def construct_dag(
    source_freshness: SourceFreshness,
    state: StateApiModel,
    manifest_override: str | None = None,
) -> ParsedDag:
    manifest = load_json(manifest_override or "target/manifest.json")

    nodes: dict[str, Node] = {}
    edges: list[Edge] = []

    project_name_from_manifest = manifest["metadata"]["project_name"]

    for node_id in manifest.get("child_map", {}).keys():
        node_id = str(node_id)
        if not node_id.startswith("source."):
            continue
        nodes[node_id] = SourceNode(last_updated=source_freshness.sources.get(node_id))

    for node_id, node in manifest.get("nodes", {}).items():
        resource_type = str(node.get("resource_type"))

        match resource_type:
            case "seed" | "model" | "snapshot":
                node_id: str = str(node_id)
                asset_external_id = node_id
                if integration_account_id := get_integration_account_id_from_env():
                    asset_external_id = f"{integration_account_id}.{node_id}"

                dbt_path = str(node["original_file_path"])
                if node["package_name"] != project_name_from_manifest:
                    file_path = f"dbt_packages/{node['package_name']}/{dbt_path}"
                else:
                    file_path = dbt_path

                track_state = True
                checksum: str | None = calculate_checksum(
                    resource_type,
                    node_checksum=str(node["checksum"]["checksum"]),
                    file_path=file_path,
                )
                if not checksum:
                    track_state = False
                    checksum = str(node["checksum"]["checksum"])

                freshness, reason = calculate_freshness_on_node(
                    asset_external_id,
                    checksum,
                    state,
                    resource_type,
                    track_state,
                )

                nodes[node_id] = MaterialisationNode(
                    checksum=checksum,
                    freshness_config=parse_freshness_config(
                        config_on_node=node.get("config", {}).get("freshness")
                    ),
                    freshness=freshness,
                    dbt_path=dbt_path,
                    reason=reason,
                    sources=(
                        state.state[asset_external_id].sources
                        if asset_external_id in state.state
                        else {}
                    ),
                    file_path=file_path,
                    last_updated=(
                        state.state[asset_external_id].last_updated
                        if asset_external_id in state.state
                        else None
                    ),
                )

                for dep in node.get("depends_on", {}).get("nodes", []):
                    edges.append(Edge(from_=str(dep), to_=node_id))
            case _:
                continue

    return ParsedDag(nodes=nodes, edges=edges)
