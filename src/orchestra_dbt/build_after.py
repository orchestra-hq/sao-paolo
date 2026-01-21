from collections import defaultdict, deque
from typing import cast

from .models import FreshnessConfig, MaterialisationNode, Node, NodeType, ParsedDag


def parse_build_after_duration_minutes(build_after: dict[str, str | int]) -> int:
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


def parse_freshness_config(config_on_node: dict | None) -> FreshnessConfig:
    freshness_config = FreshnessConfig()
    if config_on_node and "build_after" in config_on_node:
        build_after_config = config_on_node["build_after"]
        if "updates_on" in build_after_config:
            updates_on = str(build_after_config["updates_on"]).lower()
            match updates_on:
                case "any" | "all":
                    freshness_config.updates_on = updates_on
                case _:
                    freshness_config.updates_on = "any"
        freshness_config.minutes_sla = parse_build_after_duration_minutes(
            build_after_config
        )
    return freshness_config


def _build_reverse_dependency_graphs(
    dag: ParsedDag,
) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, int]]:
    """
    Build reverse dependency graph structures from DAG edges for backwards traversal.

    Returns:
        - children: parent -> list of children
        - parents: child -> list of parents
        - out_degree: node -> number of downstream dependencies (children count)
    """
    children: dict[str, list[str]] = defaultdict(list)
    parents: dict[str, list[str]] = defaultdict(list)
    out_degree: dict[str, int] = defaultdict(int)

    for edge in dag.edges:
        parent, child = edge.from_, edge.to_
        children[parent].append(child)
        parents[child].append(parent)
        out_degree[parent] += 1

    # Initialize out_degree for all nodes (nodes with no children will have 0)
    for node_id in dag.nodes.keys():
        if node_id not in out_degree:
            out_degree[node_id] = 0

    return children, parents, out_degree


def _propagate_config_to_node(
    node_id: str, children: dict[str, list[str]], dag: ParsedDag
) -> None:
    """
    Propagate freshness config from children to the current node.
    A parent only inherits the minimum config of its children if ALL children
    have a defined config; if any child is undefined or has no config, the
    parent remains null.
    """
    node: Node = dag.nodes[node_id]
    if node.node_type != NodeType.MATERIALISATION:
        return

    materialisation_node: MaterialisationNode = cast(MaterialisationNode, node)

    # If the node already has a config, don't change it
    if materialisation_node.freshness_config.minutes_sla is not None:
        return

    child_ids = children.get(node_id, [])
    if not child_ids:
        return

    # Collect minutes_sla from children; if any child has no config, do not propagate
    child_minutes: list[int] = []
    for child_id in child_ids:
        child_node = dag.nodes.get(child_id)
        if child_node is None or child_node.node_type != NodeType.MATERIALISATION:
            return  # undefined or non-materialisation child: parent stays null
        child_materialisation: MaterialisationNode = cast(
            MaterialisationNode, child_node
        )
        if child_materialisation.freshness_config.minutes_sla is None:
            return  # child has no config: parent stays null
        child_minutes.append(child_materialisation.freshness_config.minutes_sla)

    # All children have a defined config: inherit the minimum
    min_sla = min(child_minutes)
    materialisation_node.freshness_config.minutes_sla = min_sla
    for cid in child_ids:
        if (
            cast(MaterialisationNode, dag.nodes[cid]).freshness_config.minutes_sla
            == min_sla
        ):
            materialisation_node.freshness_config.inherited_from = cid
            break


def _enqueue_parents(
    node_id: str,
    parents: dict[str, list[str]],
    out_degree: dict[str, int],
    queue: deque[str],
) -> None:
    """Enqueue parent nodes for processing after the current node is processed."""
    for parent in parents[node_id]:
        out_degree[parent] -= 1
        if out_degree[parent] == 0:
            queue.append(parent)


def propagate_freshness_config(parsed_dag: ParsedDag) -> None:
    """
    Propagate freshness config backwards through the DAG.

    Starts at nodes with no children (end of DAG) and works backwards,
    propagating the minimum minutes_sla from children to parents.
    Only updates nodes that don't already have a config.
    """
    children, parents, out_degree = _build_reverse_dependency_graphs(parsed_dag)

    # Queue for nodes with no children (out_degree 0) - these are at the end of the DAG
    queue = deque[str]([n for n in parsed_dag.nodes.keys() if out_degree[n] == 0])

    while queue:
        current = queue.popleft()

        # Process the node to potentially propagate config from children
        _propagate_config_to_node(node_id=current, children=children, dag=parsed_dag)

        # Queue parents for processing (work backwards)
        _enqueue_parents(
            node_id=current, parents=parents, out_degree=out_degree, queue=queue
        )
