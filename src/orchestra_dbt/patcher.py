import os
import re
from pathlib import Path

from orchestra_dbt.models import ORCHESTRA_REUSED_NODE

from .utils import log_warn


def _patch_file(file_path: Path):
    content = file_path.read_text(encoding="utf-8")

    config_pattern = r"\{\{\s*config\s*\(\s*tags\s*=\s*\[.*?\]\s*\)\s*\}\}\s*\n?"

    if re.search(config_pattern, content):
        new_config = "{{{{ config(tags=[{}]) }}}}\n\n".format(
            f'"{ORCHESTRA_REUSED_NODE}"'
        )
        new_content = re.sub(config_pattern, "", content, flags=re.MULTILINE)
        new_content = new_config + new_content.lstrip()
    else:
        new_content = (
            "{{{{ config(tags=[{}]) }}}}".format(f'"{ORCHESTRA_REUSED_NODE}"')
            + "\n\n"
            + content
        )

    file_path.write_text(new_content, encoding="utf-8")


def patch_sql_files(model_files_to_run: list[str]):
    cwd = Path(os.getcwd())
    sql_files = list[Path](cwd.rglob("*.sql"))

    if not sql_files:
        log_warn("No SQL files found in project directory.")
        return

    for sql_file in sql_files:
        relative_path = str(sql_file.relative_to(cwd))
        if (
            relative_path.startswith("models")
            and relative_path not in model_files_to_run
        ):
            try:
                _patch_file(sql_file)
            except Exception as e:
                log_warn(f"Failed to patch {sql_file}: {e}")
