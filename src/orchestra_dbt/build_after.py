from collections import defaultdict, deque
from typing import Literal, cast

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


def _parse_updates_on(value: object) -> Literal["any", "all"]:
    return "all" if str(value).lower() == "all" else "any"


def parse_freshness_config(
    config: dict | None, node_id: str | None = None
) -> FreshnessConfig:
    """Build a node's rebuild window from its dbt `config`.

    dbt State's `state.lag_tolerance` / `state.require_fresh_data_from` take
    precedence; each falls back independently to the deprecated
    `freshness.build_after` count/period and `updates_on`.
    """
    freshness_config = FreshnessConfig()
    if not config:
        return freshness_config

    state_config = config.get("state") or {}
    build_after_config = (config.get("freshness") or {}).get("build_after") or {}

    lag_tolerance = state_config.get("lag_tolerance")
    lag_tolerance_seconds: int | None = None
    if lag_tolerance is not None:
        try:
            lag_tolerance_seconds = parse_lag_tolerance_seconds(lag_tolerance)
        except ValueError as e:
            # One bad value shouldn't abort the whole run; this node just loses it.
            log_warn(f"Ignoring state.lag_tolerance on {node_id or 'node'}: {e}")

    if lag_tolerance_seconds is not None:
        freshness_config.sla_seconds = lag_tolerance_seconds
        freshness_config.source = "lag_tolerance"
        freshness_config.bypass_on_upstream_code_change = True
    elif build_after_config:
        freshness_config.sla_seconds = (
            parse_build_after_duration_minutes(build_after_config) * 60
        )
        freshness_config.source = "build_after"

    if state_config.get("require_fresh_data_from") is not None:
        freshness_config.updates_on = _parse_updates_on(
            state_config["require_fresh_data_from"]
        )
    elif (
        isinstance(lag_tolerance, dict) and lag_tolerance.get("updates_on") is not None
    ):
        freshness_config.updates_on = _parse_updates_on(lag_tolerance["updates_on"])
    elif build_after_config.get("updates_on") is not None:
        freshness_config.updates_on = _parse_updates_on(
            build_after_config["updates_on"]
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
    A node without its own config inherits the minimum window of its children,
    but only if ALL children have one. Otherwise it falls back to
    `default_sla_seconds` (dbt State's default lag_tolerance) when set, or stays null.
    Children are always resolved first, so with a default every child has a window.
    """
    node: Node = dag.nodes[node_id]
    if node.node_type != NodeType.MATERIALISATION:
        return

    freshness_config = cast(MaterialisationNode, node).freshness_config

    # If the node already has a config, don't change it
    if freshness_config.sla_seconds is not None:
        return

    # Without its own window the node is on dbt State defaults only if one is set.
    freshness_config.bypass_on_upstream_code_change = default_sla_seconds is not None

    inherited = _min_child_config(children.get(node_id, []), dag)
    if inherited is not None:
        child_id, child_config = inherited
        freshness_config.sla_seconds = child_config.sla_seconds
        freshness_config.source = child_config.source
        freshness_config.inherited_from = child_id
    elif default_sla_seconds is not None:
        freshness_config.sla_seconds = default_sla_seconds
        freshness_config.source = "lag_tolerance"


def _min_child_config(
    child_ids: list[str], dag: ParsedDag
) -> tuple[str, FreshnessConfig] | None:
    """The child with the smallest window, or None if any child has no window."""
    child_configs: list[tuple[str, FreshnessConfig]] = []
    for child_id in child_ids:
        child_node = dag.nodes.get(child_id)
        if child_node is None or child_node.node_type != NodeType.MATERIALISATION:
            return None  # undefined or non-materialisation child
        child_config = cast(MaterialisationNode, child_node).freshness_config
        if child_config.sla_seconds is None:
            return None  # child has no config
        child_configs.append((child_id, child_config))
    if not child_configs:
        return None
    # First child wins ties.
    return min(child_configs, key=lambda c: c[1].sla_seconds or 0)


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
    Only updates nodes that don't already have a config. Nodes that inherit
    nothing get `default_sla_seconds` (the opt-in default lag_tolerance) if set.
    """
    children, parents, out_degree = _build_reverse_dependency_graphs(parsed_dag)

    # Queue for nodes with no children (out_degree 0) - these are at the end of the DAG
    queue = deque[str]([n for n in parsed_dag.nodes if out_degree[n] == 0])

    while queue:
        current = queue.popleft()

        # Process the node to potentially propagate config from children
        _propagate_config_to_node(
            node_id=current,
            children=children,
            dag=parsed_dag,
            default_sla_seconds=default_sla_seconds,
        )

        # Queue parents for processing (work backwards)
        _enqueue_parents(
            node_id=current, parents=parents, out_degree=out_degree, queue=queue
        )
