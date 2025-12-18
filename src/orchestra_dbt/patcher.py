import os
from pathlib import Path

from .constants import ORCHESTRA_REUSED_NODE
from .logger import log_info, log_warn


def patch_file(file_path: Path) -> None:
    file_path.write_text(
        "{{{{ config(tags=[{}]) }}}}\n\n".format(f'"{ORCHESTRA_REUSED_NODE}"')
        + file_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def revert_patch_file(file_path: Path) -> None:
    content = file_path.read_text(encoding="utf-8")
    # Remove the entire config line that was added by patch_file
    config_line = "{{{{ config(tags=[{}]) }}}}\n\n".format(f'"{ORCHESTRA_REUSED_NODE}"')
    content = content.replace(config_line, "", 1)  # Remove only the first occurrence
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


def patch_sql_files(sql_paths_to_patch: list[str]) -> None:
    cwd = Path(os.getcwd())
    sql_files = _get_sql_files(cwd)

    if not sql_files:
        log_warn("No SQL files found in project directory.")
        return
    else:
        log_info(msg=f"Found {len(sql_files)} SQL files in path.")

    for sql_file in sql_files:
        relative_path = str(sql_file.relative_to(cwd))
        if relative_path in sql_paths_to_patch:
            try:
                log_info(f"Patching {relative_path}...")
                patch_file(file_path=sql_file)
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
