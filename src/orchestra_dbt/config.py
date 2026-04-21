import os
import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from .state_storage import StatePersistence, StatePersistenceKind, parse_s3_uri

_TOOL_SECTION = "orchestra_dbt"


class OrchestraDbtSettings(BaseModel):
    model_config = ConfigDict(extra="ignore")

    state_file: str | None = None
    use_stateful: bool = False
    orchestra_env: Literal["app", "stage", "dev"] = "app"
    local_run: bool = False
    debug: bool = False
    integration_account_id: str | None = None

    @field_validator("orchestra_env", mode="before")
    @classmethod
    def _normalize_orchestra_env(cls, v: object) -> object:
        if isinstance(v, str):
            return v.lower()
        return v


def find_pyproject_directory(start: Path | None = None) -> Path | None:
    current = (start or Path.cwd()).resolve()
    for directory in [current, *current.parents]:
        candidate = directory / "pyproject.toml"
        if candidate.is_file():
            return directory
    return None


def _read_tool_orchestra_dbt(project_dir: Path) -> dict:
    path = project_dir / "pyproject.toml"
    with path.open("rb") as f:
        data = tomllib.load(f)
    tool = data.get("tool", {})
    if not isinstance(tool, dict):
        return {}
    section = tool.get(_TOOL_SECTION, {})
    return section if isinstance(section, dict) else {}


def _env_bool(name: str) -> bool | None:
    if name not in os.environ:
        return None
    return os.environ[name].lower() == "true"


def _env_str(name: str) -> str | None:
    if name not in os.environ:
        return None
    stripped = os.environ[name].strip()
    return stripped if stripped else None


def _merge_env_overrides(settings: OrchestraDbtSettings) -> OrchestraDbtSettings:
    use_stateful = _env_bool("ORCHESTRA_USE_STATEFUL")
    if use_stateful is not None:
        settings = settings.model_copy(update={"use_stateful": use_stateful})

    orchestra_env = _env_str("ORCHESTRA_ENV")
    if orchestra_env is not None:
        settings = settings.model_copy(update={"orchestra_env": orchestra_env})

    local_run = _env_bool("ORCHESTRA_LOCAL_RUN")
    if local_run is not None:
        settings = settings.model_copy(update={"local_run": local_run})

    if "ORCHESTRA_DBT_DEBUG" in os.environ:
        debug = os.environ["ORCHESTRA_DBT_DEBUG"].strip().lower() == "true"
        settings = settings.model_copy(update={"debug": debug})

    integration = _env_str("ORCHESTRA_INTEGRATION_ACCOUNT_ID")
    if integration is not None:
        settings = settings.model_copy(update={"integration_account_id": integration})

    return OrchestraDbtSettings.model_validate(settings.model_dump())


def load_orchestra_dbt_settings(cwd: Path | None = None) -> OrchestraDbtSettings:
    base = cwd or Path.cwd()
    project_dir = find_pyproject_directory(base)
    raw: dict = {}
    if project_dir is not None:
        raw = _read_tool_orchestra_dbt(project_dir)
    try:
        settings = OrchestraDbtSettings.model_validate(raw)
        return _merge_env_overrides(settings)
    except ValidationError as exc:
        raise ValueError(f"Invalid [tool.orchestra_dbt] configuration: {exc}") from exc


def get_integration_account_id(cwd: Path | None = None) -> str | None:
    return load_orchestra_dbt_settings(cwd).integration_account_id


def effective_state_persistence(cwd: Path | None = None) -> StatePersistence:
    if os.getenv("ORCHESTRA_API_KEY", "").strip():
        return StatePersistence(kind=StatePersistenceKind.HTTP)

    base = cwd or Path.cwd()
    env_path = os.getenv("ORCHESTRA_STATE_FILE", "").strip()
    if env_path:
        if env_path.lower().startswith("s3://"):
            bucket, key = parse_s3_uri(env_path)
            return StatePersistence(
                kind=StatePersistenceKind.S3,
                s3_bucket=bucket,
                s3_key=key,
            )
        resolved = Path(env_path).expanduser()
        if not resolved.is_absolute():
            resolved = base.resolve() / resolved
        return StatePersistence(
            kind=StatePersistenceKind.LOCAL_FILE,
            local_path=resolved.resolve(),
        )

    project_dir = find_pyproject_directory(base)
    if project_dir is None:
        return StatePersistence(kind=StatePersistenceKind.HTTP)

    tool_cfg = _read_tool_orchestra_dbt(project_dir)
    raw = tool_cfg.get("state_file")
    if not raw or not isinstance(raw, str):
        return StatePersistence(kind=StatePersistenceKind.HTTP)
    stripped = raw.strip()
    if not stripped:
        return StatePersistence(kind=StatePersistenceKind.HTTP)

    if stripped.lower().startswith("s3://"):
        bucket, key = parse_s3_uri(stripped)
        return StatePersistence(
            kind=StatePersistenceKind.S3,
            s3_bucket=bucket,
            s3_key=key,
        )

    p = Path(stripped).expanduser()
    if p.is_absolute():
        local_path = p.resolve()
    else:
        local_path = (project_dir / p).resolve()
    return StatePersistence(kind=StatePersistenceKind.LOCAL_FILE, local_path=local_path)


def effective_state_file_path(cwd: Path | None = None) -> Path | None:
    persistence = effective_state_persistence(cwd)
    if persistence.kind == StatePersistenceKind.LOCAL_FILE:
        return persistence.local_path
    return None
