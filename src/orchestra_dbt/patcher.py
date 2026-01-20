import json
import os
import re
from datetime import datetime
from pathlib import Path

from .constants import ORCHESTRA_REUSED_NODE
from .logger import log_debug, log_warn
from .models import MaterialisationNode


def patch_file(
    file_path: Path,
    reason: str,
    freshness: int | None,
    last_updated: datetime | None,
) -> None:
    """
    This function should add the following config to the top of the file:
    ```
    {{
      config(
        tags=[{ORCHESTRA_REUSED_NODE}],
        meta={
          'orchestra_reused_reason': '{reason}',
          'orchestra_freshness': '{freshness}',
          'orchestra_last_updated': '{last_updated}'
        }
      )
    }}
    ```
    """

    meta_dict: dict[str, str | int] = {
        "orchestra_reused_reason": reason.replace("'", "").replace('"', "")
    }
    if freshness:
        meta_dict["orchestra_freshness"] = freshness
    if last_updated:
        meta_dict["orchestra_last_updated"] = last_updated.isoformat()

    # Replace double quotes with single quotes
    meta_config: str = json.dumps(meta_dict).replace('"', "'")

    file_path.write_text(
        f'{{{{ config(tags=["{ORCHESTRA_REUSED_NODE}"], meta={meta_config}) }}}}\n\n'
        + file_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def revert_patch_file(file_path: Path) -> None:
    # This should remove the config added by `patch_file`.
    content = file_path.read_text(encoding="utf-8")

    # Match the config line with meta dict (handles both single and double quotes)
    pattern = (
        re.escape('{{ config(tags=["')
        + re.escape(ORCHESTRA_REUSED_NODE)
        + re.escape('"], meta=')
        + r".*?"
        + re.escape(") }}\n\n")
    )
    content = re.sub(pattern, "", content, count=1)  # Remove only the first occurrence
    file_path.write_text(content, encoding="utf-8")


def _get_sql_files(cwd: Path) -> list[Path]:
    sql_files: list[Path] = []
    for root, _, files in os.walk(cwd, followlinks=True):
        # Skip .venv directories
        if ".venv" in root:
            continue
        for name in files:
            if name.endswith(".sql"):
                sql_files.append(Path(root) / name)
    return sql_files


def patch_sql_files(nodes_to_reuse: dict[str, MaterialisationNode]) -> None:
    cwd = Path(os.getcwd())
    sql_files = _get_sql_files(cwd)

    if not sql_files:
        log_warn("No SQL files found in project directory.")
        return

    sql_paths_to_nodes: dict[str, MaterialisationNode] = {
        node.sql_path: node for node in nodes_to_reuse.values()
    }

    for sql_file in sql_files:
        relative_path = str(sql_file.relative_to(cwd))
        if relative_path in sql_paths_to_nodes:
            node: MaterialisationNode = sql_paths_to_nodes[relative_path]
            try:
                log_debug(f"Patching {relative_path}...")
                patch_file(
                    file_path=sql_file,
                    reason=node.reason,
                    freshness=node.freshness_config.minutes_sla,
                    last_updated=node.last_updated,
                )
            except Exception as e:
                log_warn(f"Failed to add tag to {sql_file}: {e}")


def revert_patching(sql_paths_to_revert: list[str]) -> None:
    cwd = Path(os.getcwd())
    sql_files = _get_sql_files(cwd)

    for sql_file in sql_files:
        relative_path = str(sql_file.relative_to(cwd))
        if relative_path in sql_paths_to_revert:
            try:
                revert_patch_file(file_path=sql_file)
            except Exception as e:
                log_warn(f"Failed to reset tag from {sql_file}: {e}")
