import os
from pathlib import Path

from .models import ORCHESTRA_REUSED_NODE, Node
from .utils import log_warn


def patch_file(file_path: Path):
    file_path.write_text(
        "{{{{ config(tags=[{}]) }}}}\n\n".format(f'"{ORCHESTRA_REUSED_NODE}"')
        + file_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def patch_sql_files(models_to_reuse: list[Node]):
    cwd = Path(os.getcwd())
    sql_files = list[Path](cwd.rglob("*.sql"))
    if not sql_files:
        log_warn("No SQL files found in project directory.")
        return

    sql_files_to_patch: list[str] = [n.sql_path for n in models_to_reuse if n.sql_path]
    for sql_file in sql_files:
        relative_path = str(sql_file.relative_to(cwd))
        if relative_path.startswith("models") and relative_path in sql_files_to_patch:
            try:
                patch_file(file_path=sql_file)
            except Exception as e:
                log_warn(f"Failed to add exclusion tag to {sql_file}: {e}")
