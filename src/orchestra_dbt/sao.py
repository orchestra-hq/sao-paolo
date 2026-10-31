from collections import defaultdict, deque
from datetime import datetime
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
    for node_id in dag.nodes:
        if node_id not in in_degree:
            in_degree[node_id] = 0

    return children, parents, in_degree


def _format_seconds(seconds: int) -> str:
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds and seconds % size == 0:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


def upstream_code_changed(
    upstream_node: Node, selected_paths: list[str] | None = None
) -> bool:
    """True when the upstream model is rebuilt in this run because its code changed.

    A parent outside the run's selection isn't rebuilt, so its children would only
    re-read its old output; it keeps its changed checksum until it is built.
    """
    if upstream_node.node_type != NodeType.MATERIALISATION:
        return False
    materialisation_node = cast(MaterialisationNode, upstream_node)
    if selected_paths and materialisation_node.dbt_path not in selected_paths:
        return False
    return (
        materialisation_node.freshness == Freshness.DIRTY
        and materialisation_node.code_changed
    )


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
                        reason = f"Source {upstream_id} has no new data since last run."
        case NodeType.MATERIALISATION:
            materialisation_node: MaterialisationNode = cast(
                MaterialisationNode, upstream_node
            )
            upstream_freshness = materialisation_node.freshness
            if upstream_freshness == Freshness.CLEAN:
                reason = "Upstream node(s) being reused."

    # An SLA (lag_tolerance / build_after) only holds a node back inside its window;
    # it is not required for the catch-up comparison below.
    sla_seconds = current_node.freshness_config.sla_seconds
    if sla_seconds is not None:
        seconds_since_last_updated = (
            datetime.now(tz=current_node.last_updated.tzinfo)
            - current_node.last_updated
        ).total_seconds()

        if seconds_since_last_updated < sla_seconds:
            minutes_since_last_updated = int(seconds_since_last_updated / 60)
            if current_node.freshness_config.source == "lag_tolerance":
                reason = f"Model still within lag_tolerance of {_format_seconds(sla_seconds)}. Last updated {minutes_since_last_updated} minutes ago."
            else:
                reason = f"Model still within freshness config of {sla_seconds // 60} minutes. Last updated {minutes_since_last_updated} minutes ago."
            if current_node.freshness_config.inherited_from:
                reason += f" Config inherited from {current_node.freshness_config.inherited_from}."
            return (False, reason)

    # A clean upstream still forces a rebuild if it ran more recently than this node
    # (e.g. a concurrent pipeline with a narrower selector advanced the upstream alone),
    # otherwise this node would never catch up to the upstream's newer output.
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
    selected_paths: list[str] | None = None,
) -> tuple[bool, str | None]:
    # dbt State: a direct parent's code change forces a rebuild even inside the
    # lag_tolerance window. build_after keeps its original behaviour.
    if node.freshness_config.bypass_on_upstream_code_change:
        for upstream_id in upstream_ids:
            if upstream_code_changed(dag.nodes[upstream_id], selected_paths):
                return True, None

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
                f"{reason} (node requires all upstream dependents to have new data).",
            )
        if node.freshness_config.updates_on == "any" and should_be_dirty:
            return True, None
    return should_be_dirty, reason


def _process_node(
    current_id: str,
    parents: dict[str, list[str]],
    dag: ParsedDag,
    selected_paths: list[str] | None = None,
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
            upstream_ids=parents[current_id],
            node=materialisation_node,
            dag=dag,
            selected_paths=selected_paths,
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


def calculate_nodes_to_run(dag: ParsedDag, selected_paths: list[str] | None = None):
    children, parents, in_degree = build_dependency_graphs(dag)

    # Queue for nodes with no upstream dependencies (in_degree 0)
    queue = deque[str]([n for n in dag.nodes if in_degree[n] == 0])

    while queue:
        current = queue.popleft()

        # Process the node to potentially mark it as dirty
        _process_node(
            current_id=current,
            parents=parents,
            dag=dag,
            selected_paths=selected_paths,
        )

        # Queue children for processing
        _enqueue_children(
            node_id=current, children=children, in_degree=in_degree, queue=queue
        )
