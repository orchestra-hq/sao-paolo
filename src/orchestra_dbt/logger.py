from collections import Counter
from datetime import UTC, datetime

import click

from .config import load_orchestra_dbt_settings
from .constants import SERVICE_NAME
from .models import MaterialisationNode


def _log(msg: str, fg: str | None, error: bool = False) -> None:
    text = str(datetime.now(UTC).strftime("%H:%M:%S")) + " "
    if error:
        text += " [ERROR]"
    text += f" [{SERVICE_NAME}] {msg}"
    click.echo(message=click.style(text=text, fg=fg), color=True)


def log_debug(msg) -> None:
    if load_orchestra_dbt_settings().debug:
        _log(msg, None)


def log_info(msg) -> None:
    _log(msg, None)


def log_warn(msg) -> None:
    _log(msg, "yellow")


def log_error(msg) -> None:
    _log(msg, "red", error=True)


def log_reused_nodes(nodes_to_reuse: dict[str, MaterialisationNode]) -> None:
    total_nodes: int = len(nodes_to_reuse)
    log_info(f"{total_nodes} node(s) to be reused:")
    counter = 1
    for node_id, node in nodes_to_reuse.items():
        log_info(
            f"{counter} of {total_nodes} REUSED {node_id} - {node.reason} (last updated: {node.last_updated or 'none'})"
        )
        counter += 1


def log_why_not_reused(
    candidates: dict[str, MaterialisationNode],
    nodes_to_reuse: dict[str, MaterialisationNode],
) -> None:
    """Break down why candidate nodes were rebuilt, grouped by reason.

    Each node carries its own `reason`, but only reused nodes ever print one -- so a run
    that reuses nothing says nothing at all about why, which is the case you most need
    to debug. Counts only nodes this run would have built, so they reconcile against
    `x/y nodes reused` rather than quietly including nodes that were never candidates.
    """
    reasons = Counter(
        node.reason
        for node_id, node in candidates.items()
        if node_id not in nodes_to_reuse
    )
    if not reasons:
        return
    log_debug(f"{sum(reasons.values())} node(s) not reused, by reason:")
    for reason, count in reasons.most_common():
        log_debug(f"  {count} node(s): {reason}")
