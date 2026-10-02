import tomllib
from pathlib import Path

from .target_finder import project_path

_TOOL_SECTION = "orchestra_dbt"


def find_pyproject_directory(start: Path | None = None) -> Path | None:
    """Nearest pyproject.toml at or above `start`; by default the dbt project's,
    falling back to the cwd's when the project dir is outside the cwd's tree."""
    if start is None:
        return find_pyproject_directory(project_path(".")) or find_pyproject_directory(
            Path.cwd()
        )
    current = start.resolve()
    for directory in [current, *current.parents]:
        candidate = directory / "pyproject.toml"
        if candidate.is_file():
            return directory
    return None


def read_orchestra_dbt_tool_config(project_dir: Path) -> dict:
    path = project_dir / "pyproject.toml"
    with path.open("rb") as f:
        data = tomllib.load(f)
    tool = data.get("tool", {})
    if not isinstance(tool, dict):
        return {}
    section = tool.get(_TOOL_SECTION, {})
    return section if isinstance(section, dict) else {}
