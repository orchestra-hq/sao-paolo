import os
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


def read_tool_orchestra_dbt(project_dir: Path) -> dict:
    path = project_dir / "pyproject.toml"
    with path.open("rb") as f:
        data = tomllib.load(f)
    tool = data.get("tool", {})
    if not isinstance(tool, dict):
        return {}
    section = tool.get(_TOOL_SECTION, {})
    return section if isinstance(section, dict) else {}


def effective_state_file_path(cwd: Path | None = None) -> Path | None:
    env_path = os.getenv("ORCHESTRA_STATE_FILE", "").strip()
    base = cwd or Path.cwd()
    if env_path:
        resolved = Path(env_path).expanduser()
        if not resolved.is_absolute():
            resolved = base.resolve() / resolved
        return resolved.resolve()

    project_dir = find_pyproject_directory(base)
    if project_dir is None:
        return None

    tool_cfg = read_tool_orchestra_dbt(project_dir)
    raw = tool_cfg.get("state_file")
    if not raw or not isinstance(raw, str):
        return None
    stripped = raw.strip()
    if not stripped:
        return None

    p = Path(stripped).expanduser()
    if p.is_absolute():
        return p.resolve()
    return (project_dir / p).resolve()
