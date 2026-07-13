import tomllib
from pathlib import Path

_TOOL_SECTION = "orchestra_dbt"


def find_pyproject_directory(start: Path | None = None) -> Path | None:
    current = (start or Path.cwd()).resolve()
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
