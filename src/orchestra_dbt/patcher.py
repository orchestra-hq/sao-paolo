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


def patch_sql_files(sql_paths_to_patch: list[str]) -> None:
    cwd = Path(os.getcwd())
    sql_files = list[Path](cwd.rglob("*.sql"))
    if not sql_files:
        log_warn("No SQL files found in project directory.")
        return
    else:
        log_info(f"Found {len(sql_files)} to process.")

    for sql_file in sql_files:
        relative_path = str(sql_file.relative_to(cwd))
        if relative_path in sql_paths_to_patch:
            try:
                log_info(f"Patching {relative_path}...")
                patch_file(file_path=sql_file)
            except Exception as e:
                log_warn(f"Failed to add tag to {sql_file}: {e}")
