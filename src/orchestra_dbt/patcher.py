import os
from pathlib import Path

from .models import ORCHESTRA_REUSED_NODE, Node
from .utils import log_warn


def patch_file(file_path: Path) -> None:
    file_path.write_text(
        "{{{{ config(tags=[{}]) }}}}\n\n".format(f'"{ORCHESTRA_REUSED_NODE}"')
        + file_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def patch_sql_files(
    models_to_reuse: dict[str, Node], model_paths_to_run: list[str] | None
) -> None:
    cwd = Path(os.getcwd())
    sql_files = list[Path](cwd.rglob("*.sql"))
    if not sql_files:
        log_warn("No SQL files found in project directory.")
        return

    models_to_patch: dict[str, str] = {
        model.sql_path: model_id
        for model_id, model in models_to_reuse.items()
        if model.sql_path
    }

    for sql_file in sql_files:
        relative_path = str(sql_file.relative_to(cwd))
        if (
            relative_path.startswith("models")
            and relative_path in models_to_patch
            and (model_paths_to_run is None or relative_path in model_paths_to_run)
        ):
            try:
                patch_file(file_path=sql_file)
            except Exception as e:
                log_warn(f"Failed to add exclusion tag to {sql_file}: {e}")
