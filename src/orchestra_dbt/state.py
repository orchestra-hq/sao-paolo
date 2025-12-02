from collections import defaultdict, deque
from datetime import datetime, timedelta

from .call_source_freshness import source_freshness_invoke
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


def get_source_freshness() -> SourceFreshness:
    source_freshness_invoke()
    return SourceFreshness(
        sources={
            source["unique_id"]: source["max_loaded_at"]
            for source in load_file("target/sources.json")["results"]
        }
    )


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

    count = build_after["count"]
    if not isinstance(count, int):
        raise ValueError(f"Invalid count: {count}")

    return count * mins_multiplier


def _valid_sla(child: str, config: dict | None, state: StateApiModel) -> bool:
    # No freshness config
    if not config:
        return True

    build_after = config.get("build_after")
    if not build_after:
        return True

    model_last_updated: datetime | None = (
        state.state[child].last_updated if child in state.state else None
    )
    if not model_last_updated:
        return True

    sla_duration_mins = build_sla_duration(build_after)
    return model_last_updated + timedelta(minutes=sla_duration_mins) < datetime.now(
        model_last_updated.tzinfo
    )


def calculate_models_to_run(dag: ParsedDag, state: StateApiModel) -> ParsedDag:
    children: defaultdict[str, list[str]] = defaultdict[str, list[str]](list)
    in_degree: defaultdict[str, int] = defaultdict[str, int](int)

    for edge in dag.edges:
        parent, child = edge.from_, edge.to_
        children[parent].append(child)
        in_degree[child] += 1
        if parent not in in_degree:
            in_degree[parent] = in_degree.get(parent, 0)

    # Queue for nodes with no upstream (in_degree 0)
    queue = deque[str]([n for n in dag.nodes.keys() if in_degree[n] == 0])

    while queue:
        current = queue.popleft()

        # Propagate state to children
        for child in children[current]:
            # If current is DIRTY, child becomes DIRTY
            if dag.nodes[current].freshness == Freshness.DIRTY and _valid_sla(
                child, dag.nodes[child].freshness_config, state
            ):
                dag.nodes[child].freshness = Freshness.DIRTY

            # Decrement in-degree and add to queue if ready
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    return dag
