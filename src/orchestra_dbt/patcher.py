import os
import re
from pathlib import Path

from .constants import ORCHESTRA_REUSED_NODE
from .logger import log_info, log_warn
from .models import ModelNode


def patch_file(file_path: Path, reason: str) -> None:
    # This file should add the following config to the top of the file:
    # {{ config(tags=[{ORCHESTRA_REUSED_NODE}], meta={'orchestra_reused_reason': '{reason}'}) }}
    file_path.write_text(
        f"{{{{ config(tags=[\"{ORCHESTRA_REUSED_NODE}\"], meta={{'orchestra_reused_reason': '{reason}'}}) }}}}\n\n"
        + file_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def revert_patch_file(file_path: Path) -> None:
    # This should remove the line added by patch_file.
    content = file_path.read_text(encoding="utf-8")
    pattern = (
        re.escape('{{ config(tags=["')
        + re.escape(ORCHESTRA_REUSED_NODE)
        + re.escape("\"], meta={'orchestra_reused_reason': '")
        + r".*?"
        + re.escape("'}) }}\n\n")
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


def patch_sql_files(models_to_reuse: dict[str, ModelNode]) -> None:
    cwd = Path(os.getcwd())
    sql_files = _get_sql_files(cwd)

    if not sql_files:
        log_warn("No SQL files found in project directory.")
        return

    sql_paths_to_patch_with_reason: dict[str, str] = {
        model.sql_path: model.reason for model in models_to_reuse.values()
    }

    for sql_file in sql_files:
        relative_path = str(sql_file.relative_to(cwd))
        if relative_path in sql_paths_to_patch_with_reason:
            try:
                log_info(f"Patching {relative_path}...")
                patch_file(
                    file_path=sql_file,
                    reason=sql_paths_to_patch_with_reason[relative_path],
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
