import tomllib
from pathlib import Path

_TOOL_SECTION = "orchestra_dbt"


def find_orchestra_dbt_config(start: Path) -> tuple[Path, dict] | None:
    """The nearest [tool.orchestra_dbt] section at or above `start`, and its pyproject.toml's directory.

    Skips a pyproject.toml without the section, so a dbt project's own doesn't hide a parent's.
    """
    current = start.resolve()
    for directory in [current, *current.parents]:
        path = directory / "pyproject.toml"
        if not path.is_file():
            continue
        with path.open("rb") as f:
            tool = tomllib.load(f).get("tool", {})
        section = tool.get(_TOOL_SECTION) if isinstance(tool, dict) else None
        if isinstance(section, dict):
            return directory, section
    return None
