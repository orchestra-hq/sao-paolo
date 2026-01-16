from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import cast

from .models import (
    Freshness,
    MaterialisationNode,
    Node,
    NodeType,
    ParsedDag,
    SourceNode,
)


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


def should_mark_dirty_from_single_upstream(
    upstream_id: str, upstream_node: Node, current_node: MaterialisationNode
) -> tuple[bool, str | None]:
    if not current_node.last_updated:
        # This scenario should not occur as the node will be dirty already.
        # But helps with typing.
        return True, None

    reason = None

    match upstream_node.node_type:
        case NodeType.SOURCE:
            source_node: SourceNode = cast(SourceNode, upstream_node)
            if upstream_id not in current_node.sources:
                upstream_freshness = Freshness.DIRTY
            else:
                if not source_node.last_updated:
                    upstream_freshness = Freshness.DIRTY
                else:
                    upstream_freshness = (
                        Freshness.DIRTY
                        if source_node.last_updated > current_node.sources[upstream_id]
                        else Freshness.CLEAN
                    )
                    if upstream_freshness == Freshness.CLEAN:
                        reason = (
                            f"Source {upstream_id} has not been updated since last run."
                        )
        case NodeType.MATERIALISATION:
            materialisation_node: MaterialisationNode = cast(
                MaterialisationNode, upstream_node
            )
            upstream_freshness = materialisation_node.freshness
            if upstream_freshness == Freshness.CLEAN:
                reason = "Upstream node(s) being reused."

    if not current_node.freshness_config.minutes_sla:
        return upstream_freshness == Freshness.DIRTY, reason

    if current_node.last_updated >= datetime.now(
        tz=current_node.last_updated.tzinfo
    ) - timedelta(minutes=current_node.freshness_config.minutes_sla):
        return (
            False,
            f"Model still within build_after config of {current_node.freshness_config.minutes_sla} minutes.",
        )

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
    node: MaterialisationNode,
    dag: ParsedDag,
) -> tuple[bool, str | None]:
    should_be_dirty = False
    reason = None
    for upstream_id in upstream_ids:
        should_be_dirty, reason = should_mark_dirty_from_single_upstream(
            upstream_id=upstream_id,
            upstream_node=dag.nodes[upstream_id],
            current_node=node,
        )
        if node.freshness_config.updates_on == "all" and not should_be_dirty:
            return (
                False,
                f"{reason} (nodes requires all upstreams to be updated)",
            )
        if node.freshness_config.updates_on == "any" and should_be_dirty:
            return True, None
    return should_be_dirty, reason


def _process_node(
    current_id: str, parents: dict[str, list[str]], dag: ParsedDag
) -> None:
    """
    Process a single node to determine if it should be marked dirty based on upstream dependencies.
    """
    node: Node = dag.nodes[current_id]
    if node.node_type != NodeType.MATERIALISATION:
        return

    materialisation_node: MaterialisationNode = cast(MaterialisationNode, node)
    if materialisation_node.freshness == Freshness.CLEAN:
        should_mark_dirty, reason = _should_mark_dirty(
            upstream_ids=parents[current_id], node=materialisation_node, dag=dag
        )
        if should_mark_dirty:
            materialisation_node.freshness = Freshness.DIRTY
        else:
            if reason:
                materialisation_node.reason = reason


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


def calculate_nodes_to_run(dag: ParsedDag):
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
