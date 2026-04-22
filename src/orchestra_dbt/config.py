import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from .project_discovery import (
    find_pyproject_directory,
    read_orchestra_dbt_tool_config,
)
from .state_backend_config import StateBackendConfig, StateBackendKind
from .state_storage import StatePersistence


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
        raw = read_orchestra_dbt_tool_config(project_dir)
    try:
        settings = OrchestraDbtSettings.model_validate(raw)
        return _merge_env_overrides(settings)
    except ValidationError as exc:
        raise ValueError(f"Invalid [tool.orchestra_dbt] configuration: {exc}") from exc


def get_integration_account_id(cwd: Path | None = None) -> str | None:
    return load_orchestra_dbt_settings(cwd).integration_account_id


def resolve_state_backend_config(cwd: Path | None = None) -> StateBackendConfig:
    from .state_backends.factory import resolve_state_backend_config as _resolve

    return _resolve(cwd)


def resolve_state_file_path(cwd: Path | None = None) -> Path | None:
    cfg = resolve_state_backend_config(cwd)
    if cfg.kind == StateBackendKind.LOCAL_FILE:
        return cfg.local_path
    return None


def effective_state_file_path(cwd: Path | None = None) -> Path | None:
    return resolve_state_file_path(cwd)


def effective_state_persistence(cwd: Path | None = None) -> StatePersistence:
    cfg = resolve_state_backend_config(cwd)
    return StatePersistence.model_validate(cfg.model_dump())
