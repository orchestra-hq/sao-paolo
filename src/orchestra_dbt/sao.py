from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Literal

from .models import Freshness, Node, ParsedDag


def build_dependency_graphs(
    dag: ParsedDag,
) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, int]]:
    """
    Build dependency graph structures from DAG edges.

    Returns:
        - children: parent -> list of children (for topological sorting)
        - parents: child -> list of parents (to know upstream dependencies)
        - in_degree: node -> number of upstream dependencies
    """
    children: dict[str, list[str]] = defaultdict(list)
    parents: dict[str, list[str]] = defaultdict(list)
    in_degree: dict[str, int] = defaultdict(int)

    for edge in dag.edges:
        parent, child = edge.from_, edge.to_
        children[parent].append(child)
        parents[child].append(parent)
        in_degree[child] += 1

    # Initialize in_degree for all nodes (nodes with no dependencies will have 0)
    for node_id in dag.nodes.keys():
        if node_id not in in_degree:
            in_degree[node_id] = 0

    return children, parents, in_degree


def build_after_duration_minutes(build_after: dict[str, str | int]) -> int:
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


def _get_updates_on_mode(freshness_config: dict | None) -> Literal["any", "all"]:
    if not freshness_config:
        return "any"

    build_after = freshness_config.get("build_after")
    if not build_after:
        return "any"

    return build_after.get("updates_on", "any")


def should_mark_dirty_from_single_upstream(
    upstream_node: Node,
    freshness_config: dict | None,
) -> bool:
    """
    Check if a single upstream dependency should cause the downstream model to be marked dirty.

    Returns True if:
    - The upstream was updated more recently than (now - build_after duration)
    - OR the upstream is DIRTY itself and falls outside of the config
    """
    upstream_last_updated: datetime | None = upstream_node.last_updated
    if upstream_last_updated is None:
        return True

    if not freshness_config:
        return upstream_node.freshness == Freshness.DIRTY

    build_after = freshness_config.get("build_after")
    if not build_after:
        return upstream_node.freshness == Freshness.DIRTY

    threshold_time = datetime.now(upstream_last_updated.tzinfo) - timedelta(
        minutes=build_after_duration_minutes(build_after)
    )

    # Check if upstream was updated more recently than the threshold
    if upstream_last_updated > threshold_time:
        return True

    # Check if upstream is DIRTY and falls outside of the config
    if upstream_node.freshness == Freshness.DIRTY:
        # If upstream is dirty and was updated at or before the threshold,
        # enough time has passed and it should trigger dirty
        return upstream_last_updated <= threshold_time

    return False


def _should_mark_dirty_based_on_upstreams(
    upstream_ids: list[str],
    freshness_config: dict | None,
    dag: ParsedDag,
) -> bool:
    updates_on: Literal["any", "all"] = _get_updates_on_mode(freshness_config)

    for upstream_id in upstream_ids:
        should_be_dirty = should_mark_dirty_from_single_upstream(
            upstream_node=dag.nodes[upstream_id],
            freshness_config=freshness_config,
        )
        if updates_on == "all" and not should_be_dirty:
            return False
        if updates_on == "any" and should_be_dirty:
            return True

    return False


def _process_node(
    node_id: str,
    node: Node,
    parents: dict[str, list[str]],
    dag: ParsedDag,
) -> None:
    """
    Process a single node to determine if it should be marked dirty based on upstream dependencies.
    """
    # If already dirty, nothing to do (children will be processed separately)
    # If no upstream dependencies, nothing to check
    if node.freshness == Freshness.DIRTY or not parents[node_id]:
        return

    # Check all upstream dependencies and apply updates_on logic
    if _should_mark_dirty_based_on_upstreams(
        upstream_ids=parents[node_id],
        freshness_config=node.freshness_config,
        dag=dag,
    ):
        node.freshness = Freshness.DIRTY


def _enqueue_children(
    node_id: str,
    children: dict[str, list[str]],
    in_degree: dict[str, int],
    queue: deque[str],
) -> None:
    for child in children[node_id]:
        in_degree[child] -= 1
        if in_degree[child] == 0:
            queue.append(child)


def calculate_models_to_run(dag: ParsedDag) -> ParsedDag:
    """
    Iterates over all models starting with those that have no dependencies, moving downstream.
    Each clean model checks its upstream dependencies to determine if it should be marked dirty.
    """
    children, parents, in_degree = build_dependency_graphs(dag)

    # Queue for nodes with no upstream dependencies (in_degree 0)
    queue = deque[str]([n for n in dag.nodes.keys() if in_degree[n] == 0])

    while queue:
        current = queue.popleft()

        # Process the node to potentially mark it as dirty
        _process_node(
            node_id=current, node=dag.nodes[current], parents=parents, dag=dag
        )

        # Queue children for processing
        _enqueue_children(
            node_id=current, children=children, in_degree=in_degree, queue=queue
        )

    return dag
