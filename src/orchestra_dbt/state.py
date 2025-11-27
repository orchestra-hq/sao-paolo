from collections import defaultdict, deque
from datetime import timedelta, datetime
from enum import Enum

from orchestra_dbt.call_source_freshness import get_sf_from_dbt
from .utils import load_file, log_info
from .cache_store import get_entry
from .config import get_cache_id


class Freshness(Enum):
    CLEAN = "CLEAN"
    DIRTY = "DIRTY"


def get_sources_from_manifest(manifest: dict) -> set[str]:
    source_keys = set[str](manifest["sources"].keys())
    log_info(f"Found {len(source_keys)} sources from the manifest")
    return source_keys


def get_source_freshness(sources: set[str]) -> dict[str, str | None]:
    sources_json = load_file("target/sources.json")
    source_freshness = {}
    count_from_source_freshness_command = 0
    for result in sources_json["results"]:
        if result["unique_id"] in sources:
            count_from_source_freshness_command += 1
            source_freshness[result["unique_id"]] = result["max_loaded_at"]
            sources.remove(result["unique_id"])

    log_info(
        f"{count_from_source_freshness_command}/"
        f"{count_from_source_freshness_command + len(sources)} "
        "sources data obtained from dbt source freshness"
    )

    return source_freshness


def construct_dag(
    manifest: dict, source_freshness: dict[str, str], cache: dict
) -> dict:
    nodes = {}
    edges = []

    # compute dag from sources -> models
    for node_id, downstream_nodes in manifest.get("child_map", {}).items():
        if str(node_id).startswith("source."):
            cache_value = cache.get(node_id, {}).get("last_updated")
            freshness = (
                Freshness.DIRTY
                if not cache_value or source_freshness[node_id] > cache_value
                else Freshness.CLEAN
            )
            nodes[node_id] = {
                "type": "source",
                "sql_path": None,
                "checksum": None,
                "freshness": freshness,
                "last_updated": source_freshness[node_id],
            }
            for dep in downstream_nodes:
                if str(dep).startswith("model."):
                    edges.append({"from": node_id, "to": dep})

    # compute model DAG
    for node_id, node in manifest.get("nodes", {}).items():
        if node["resource_type"] == "model":
            cache_value = cache.get(node_id, {}).get("checksum")
            freshness = (
                Freshness.DIRTY
                if not cache_value or node["checksum"]["checksum"] != cache_value
                else Freshness.CLEAN
            )
            nodes[node_id] = {
                "type": "model",
                "sql_path": node["original_file_path"],
                "checksum": node["checksum"]["checksum"],
                "freshness": freshness,
                "config": node["config"]["freshness"],
            }
            # Add edges for dependencies
            for dep in node.get("depends_on", {}).get("nodes", []):
                if dep in manifest.get("nodes", {}):
                    edges.append({"from": dep, "to": node_id})

    return {"nodes": nodes, "edges": edges}


def build_sla_duration(build_after: dict) -> int:
    # always return in minutes
    period = build_after["period"]  # minute | hour | day
    match period:
        case "minute":
            mins_multiplier = 1
        case "hour":
            mins_multiplier = 60
        case "day":
            mins_multiplier = 1440
        case _:
            raise ValueError(f"Invalid period: {period}")
    return build_after["count"] * mins_multiplier


def valid_sla(child: str, config: dict, cache: dict) -> bool:
    # No freshness config
    if not config:
        return True

    build_after = config.get("build_after")
    if not build_after:
        return True

    model_last_updated: str | None = cache.get(child, {}).get("last_updated")
    if not model_last_updated:
        return True

    model_last_updated_datetime = datetime.fromisoformat(model_last_updated)

    sla_duration_mins = build_sla_duration(build_after)
    return (
        model_last_updated_datetime + timedelta(minutes=sla_duration_mins)
        < datetime.now()
    )


def run_sao(dag: dict, cache: dict) -> list[tuple[str, str]]:
    nodes = dag["nodes"]
    edges = dag["edges"]

    # Build adjacency list for children
    children = defaultdict(list)
    in_degree = defaultdict(int)

    for edge in edges:
        parent, child = edge["from"], edge["to"]
        children[parent].append(child)
        in_degree[child] += 1
        if parent not in in_degree:
            in_degree[parent] = in_degree.get(parent, 0)

    # Queue for nodes with no upstream (in_degree 0)
    queue = deque([n for n in nodes if in_degree[n] == 0])

    while queue:
        current = queue.popleft()
        current_state: Freshness = nodes[current]["freshness"]

        # Propagate state to children
        for child in children[current]:
            # If current is DIRTY, child becomes DIRTY
            if current_state == Freshness.DIRTY and valid_sla(
                child, nodes[child]["config"], cache
            ):
                nodes[child]["freshness"] = Freshness.DIRTY
            # Decrement in-degree and add to queue if ready
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    return nodes


def compute_models_to_run() -> tuple[list[str], dict[str, str]]:
    log_info("Running dbt source freshness")
    get_sf_from_dbt()
    manifest_json = load_file("target/manifest.json")
    sources = get_sources_from_manifest(manifest_json)
    cache = get_entry(get_cache_id())
    source_freshness = get_source_freshness(sources)
    dag = construct_dag(manifest_json, source_freshness, cache)
    nodes = run_sao(dag, cache)
    return nodes
