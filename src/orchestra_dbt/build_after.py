import re
from collections import defaultdict, deque
from typing import Literal, cast

from .models import FreshnessConfig, MaterialisationNode, Node, NodeType, ParsedDag

_UNIT_SECONDS: dict[str, float] = {
    **dict.fromkeys(("ms", "msec", "millis", "millisecond", "milliseconds"), 0.001),
    **dict.fromkeys(("s", "sec", "secs", "second", "seconds"), 1),
    **dict.fromkeys(("m", "min", "mins", "minute", "minutes"), 60),
    **dict.fromkeys(("h", "hr", "hrs", "hour", "hours"), 3600),
    **dict.fromkeys(("d", "day", "days"), 86400),
    **dict.fromkeys(("w", "week", "weeks"), 604800),
}
# dbt-oss (v2) writes lag_tolerance to the manifest as {count, period}.
_PERIOD_SECONDS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
}
_DURATION_PART = re.compile(r"(\d+)\s*([a-z]+)")
_DURATION = re.compile(r"^(?:\d+\s*[a-z]+\s*)+$")


def _parse_lag_tolerance_object(value: dict) -> int:
    count, period = value.get("count"), value.get("period")
    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        or period not in _PERIOD_SECONDS
    ):
        raise ValueError(f"Invalid lag_tolerance: {value!r}")
    return count * _PERIOD_SECONDS[str(period)]


def parse_lag_tolerance_seconds(value: str | int | dict) -> int:
    """Parse a dbt State `lag_tolerance` into seconds.

    Accepts the duration strings dbt takes (`0s`, `45m`, `1h30m`, `4 hours`,
    `120sec`, `2w`), the `{count, period}` object dbt-oss writes to the manifest,
    and a bare integer as seconds.
    """
    if isinstance(value, dict):
        return _parse_lag_tolerance_object(value)
    if isinstance(value, int) and not isinstance(value, bool):
        if value < 0:
            raise ValueError(f"Invalid lag_tolerance: {value!r}")
        return value
    text = str(value).strip().lower()
    if not _DURATION.match(text):
        raise ValueError(
            f"Invalid lag_tolerance: {value!r}. Expected a duration such as '45m' or '1h30m'."
        )
    total = 0.0
    for count, unit in _DURATION_PART.findall(text):
        if unit not in _UNIT_SECONDS:
            raise ValueError(
                f"Invalid lag_tolerance: {value!r}. Unknown unit {unit!r}."
            )
        total += int(count) * _UNIT_SECONDS[unit]
    return int(total)


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
    if lag_tolerance is not None:
        try:
            freshness_config.sla_seconds = parse_lag_tolerance_seconds(lag_tolerance)
        except ValueError as e:
            raise ValueError(f"{node_id or 'node'}: {e}") from e
        freshness_config.source = "lag_tolerance"
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


def apply_default_lag_tolerance(parsed_dag: ParsedDag, sla_seconds: int) -> None:
    """Give every model without an explicit or inherited window dbt State's default."""
    for node in parsed_dag.nodes.values():
        if node.node_type != NodeType.MATERIALISATION:
            continue
        freshness_config = cast(MaterialisationNode, node).freshness_config
        if freshness_config.sla_seconds is None:
            freshness_config.sla_seconds = sla_seconds
            freshness_config.source = "lag_tolerance"


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
    if materialisation_node.freshness_config.sla_seconds is not None:
        return

    child_ids = children.get(node_id, [])
    if not child_ids:
        return

    # Collect sla_seconds from children; if any child has no config, do not propagate
    child_configs: list[tuple[str, FreshnessConfig]] = []
    for child_id in child_ids:
        child_node = dag.nodes.get(child_id)
        if child_node is None or child_node.node_type != NodeType.MATERIALISATION:
            return  # undefined or non-materialisation child: parent stays null
        child_config = cast(MaterialisationNode, child_node).freshness_config
        if child_config.sla_seconds is None:
            return  # child has no config: parent stays null
        child_configs.append((child_id, child_config))

    # All children have a defined config: inherit the minimum (first child wins ties)
    min_child_id, min_config = min(child_configs, key=lambda c: c[1].sla_seconds or 0)
    materialisation_node.freshness_config.sla_seconds = min_config.sla_seconds
    materialisation_node.freshness_config.source = min_config.source
    materialisation_node.freshness_config.inherited_from = min_child_id


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
    propagating the minimum sla_seconds from children to parents.
    Only updates nodes that don't already have a config.
    """
    children, parents, out_degree = _build_reverse_dependency_graphs(parsed_dag)

    # Queue for nodes with no children (out_degree 0) - these are at the end of the DAG
    queue = deque[str]([n for n in parsed_dag.nodes if out_degree[n] == 0])

    while queue:
        current = queue.popleft()

        # Process the node to potentially propagate config from children
        _propagate_config_to_node(node_id=current, children=children, dag=parsed_dag)

        # Queue parents for processing (work backwards)
        _enqueue_parents(
            node_id=current, parents=parents, out_degree=out_degree, queue=queue
        )
