from collections import defaultdict, deque
from typing import cast

from .durations import parse_lag_tolerance_seconds
from .logger import log_warn
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
        raise TypeError(f"Invalid count: {count}")

    return count * mins_multiplier


def parse_freshness_config(config: dict, node_id: str = "") -> FreshnessConfig:
    """dbt State's `state` block wins; each key falls back to `build_after`."""
    freshness_config = FreshnessConfig()
    state = config.get("state") or {}
    build_after = (config.get("freshness") or {}).get("build_after") or {}

    if state.get("lag_tolerance") is not None:
        try:
            freshness_config.sla_seconds = parse_lag_tolerance_seconds(
                state["lag_tolerance"]
            )
            freshness_config.bypass_on_upstream_code_change = True
        except ValueError as e:
            log_warn(f"Ignoring state.lag_tolerance on {node_id}: {e}")
    if freshness_config.sla_seconds is None and build_after:
        freshness_config.sla_seconds = (
            parse_build_after_duration_minutes(build_after) * 60
        )

    updates_on = state.get("require_fresh_data_from") or build_after.get("updates_on")
    if str(updates_on).lower() == "all":
        freshness_config.updates_on = "all"
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
    for node_id in dag.nodes:
        if node_id not in out_degree:
            out_degree[node_id] = 0

    return children, parents, out_degree


def _propagate_config_to_node(
    node_id: str,
    children: dict[str, list[str]],
    dag: ParsedDag,
    default_sla_seconds: int | None = None,
) -> None:
    """
    Propagate freshness config from children to the current node.
    A parent only inherits the minimum config of its children if ALL children
    have a defined config; otherwise it gets `default_sla_seconds` (may be None).
    """
    node: Node = dag.nodes[node_id]
    if node.node_type != NodeType.MATERIALISATION:
        return

    materialisation_node: MaterialisationNode = cast(MaterialisationNode, node)

    # If the node already has a config, don't change it
    if materialisation_node.freshness_config.sla_seconds is not None:
        return

    # Without its own config, a node is on dbt State behaviour only via the default.
    materialisation_node.freshness_config.sla_seconds = default_sla_seconds
    materialisation_node.freshness_config.bypass_on_upstream_code_change = (
        default_sla_seconds is not None
    )

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
        if child_materialisation.freshness_config.sla_seconds is None:
            return  # child has no config: parent stays null
        child_minutes.append(child_materialisation.freshness_config.sla_seconds)

    # All children have a defined config: inherit the minimum
    min_sla = min(child_minutes)
    if default_sla_seconds is not None and default_sla_seconds < min_sla:
        return  # the default is already the smaller window
    materialisation_node.freshness_config.sla_seconds = min_sla
    for cid in child_ids:
        if (
            cast(MaterialisationNode, dag.nodes[cid]).freshness_config.sla_seconds
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


def propagate_freshness_config(
    parsed_dag: ParsedDag, default_sla_seconds: int | None = None
) -> None:
    """
    Propagate freshness config backwards through the DAG.

    Starts at nodes with no children (end of DAG) and works backwards,
    propagating the minimum sla_seconds from children to parents.
    Only updates nodes that don't already have a config.
    """
    children, parents, out_degree = _build_reverse_dependency_graphs(parsed_dag)

    # Queue for nodes with no children (out_degree 0) - these are at the end of the DAG
    queue = deque[str]([n for n in parsed_dag.nodes if out_degree[n] == 0])

    while queue:
        current = queue.popleft()

        # Process the node to potentially propagate config from children
        _propagate_config_to_node(current, children, parsed_dag, default_sla_seconds)

        # Queue parents for processing (work backwards)
        _enqueue_parents(
            node_id=current, parents=parents, out_degree=out_degree, queue=queue
        )
