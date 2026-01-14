from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Literal, cast

from .models import Freshness, ModelNode, Node, NodeType, ParsedDag


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


def _get_updates_on(freshness_config: dict | None) -> Literal["any", "all"]:
    if not freshness_config:
        return "any"

    build_after = freshness_config.get("build_after")
    if not build_after:
        return "any"

    return build_after.get("updates_on", "any")


def should_mark_dirty_from_single_upstream(
    upstream_id: str, upstream_node: Node, current_node: ModelNode
) -> tuple[bool, str | None]:
    if not current_node.last_updated:
        # This scenario should not occur as the node will be dirty already.
        # But helps with typing.
        return True, None

    reason = None

    # Based on if the upstream is a source or a node, calculate if it is dirty or not.
    if upstream_node.node_type == NodeType.SOURCE:
        if upstream_id not in current_node.sources:
            upstream_freshness = Freshness.DIRTY
        else:
            if not upstream_node.last_updated:
                upstream_freshness = Freshness.DIRTY
            else:
                upstream_freshness = (
                    Freshness.DIRTY
                    if upstream_node.last_updated > current_node.sources[upstream_id]
                    else Freshness.CLEAN
                )
                if upstream_freshness == Freshness.CLEAN:
                    reason = (
                        f"Source {upstream_id} has not been updated since last run."
                    )
    elif upstream_node.node_type == NodeType.MODEL:
        model_node: ModelNode = cast(ModelNode, upstream_node)
        upstream_freshness = model_node.freshness
        if upstream_freshness == Freshness.CLEAN:
            reason = "Upstream model(s) being reused."
    else:
        return True, None

    if not current_node.freshness_config:
        return upstream_freshness == Freshness.DIRTY, reason

    # Similar to above - build_after should always exist, so this is more for
    # type checking.
    build_after = current_node.freshness_config.get("build_after")
    if not build_after:
        return upstream_freshness == Freshness.DIRTY, reason

    minutes_sla = build_after_duration_minutes(build_after)
    if current_node.last_updated >= datetime.now(
        tz=current_node.last_updated.tzinfo
    ) - timedelta(minutes=minutes_sla):
        return False, f"Model still within build_after config of {minutes_sla} minutes."

    match upstream_freshness:
        case Freshness.DIRTY:
            return True, reason
        case Freshness.CLEAN:
            return (
                (True, None)
                if (
                    upstream_node.last_updated
                    and upstream_node.last_updated > current_node.last_updated
                )
                else (False, reason)
            )


def _should_mark_dirty(
    upstream_ids: list[str],
    node: ModelNode,
    dag: ParsedDag,
) -> tuple[bool, str | None]:
    updates_on: Literal["any", "all"] = _get_updates_on(node.freshness_config)
    should_be_dirty = False
    reason = None
    for upstream_id in upstream_ids:
        should_be_dirty, reason = should_mark_dirty_from_single_upstream(
            upstream_id=upstream_id,
            upstream_node=dag.nodes[upstream_id],
            current_node=node,
        )
        if updates_on == "all" and not should_be_dirty:
            return False, f"{reason}. Model requires all upstream models to be updated."
        if updates_on == "any" and should_be_dirty:
            return True, None
    return should_be_dirty, reason


def _process_node(
    current_id: str, parents: dict[str, list[str]], dag: ParsedDag
) -> None:
    """
    Process a single node to determine if it should be marked dirty based on upstream dependencies.
    """
    node: Node = dag.nodes[current_id]
    if node.node_type == NodeType.SOURCE:
        return

    model_node: ModelNode = cast(ModelNode, node)
    if model_node.freshness == Freshness.CLEAN:
        should_mark_dirty, reason = _should_mark_dirty(
            upstream_ids=parents[current_id], node=model_node, dag=dag
        )
        if should_mark_dirty:
            model_node.freshness = Freshness.DIRTY
        else:
            if reason:
                model_node.reason = reason


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


def calculate_models_to_run(dag: ParsedDag):
    children, parents, in_degree = build_dependency_graphs(dag)

    # Queue for nodes with no upstream dependencies (in_degree 0)
    queue = deque[str]([n for n in dag.nodes.keys() if in_degree[n] == 0])

    while queue:
        current = queue.popleft()

        # Process the node to potentially mark it as dirty
        _process_node(current_id=current, parents=parents, dag=dag)

        # Queue children for processing
        _enqueue_children(
            node_id=current, children=children, in_degree=in_degree, queue=queue
        )
