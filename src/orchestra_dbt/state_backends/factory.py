from pathlib import Path

from ..config import (
    get_orchestra_api_key,
    get_orchestra_state_file_env_override,
    load_orchestra_dbt_settings,
)
from ..project_discovery import find_pyproject_directory
from ..state_types import (
    StateBackendConfig,
    StateBackendKind,
    backend_config_from_state_location,
)
from .base import StateBackend
from .http import HttpStateBackend
from .local_file import LocalFileStateBackend


def resolve_state_backend_config(cwd: Path | None = None) -> StateBackendConfig:
    if get_orchestra_api_key():
        return StateBackendConfig(kind=StateBackendKind.HTTP)

    base = cwd or Path.cwd()
    env_path = get_orchestra_state_file_env_override()
    if env_path:
        return backend_config_from_state_location(
            env_path, resolve_relative_from=base.resolve()
        )

    project_dir = find_pyproject_directory(base)
    if project_dir is None:
        return StateBackendConfig(kind=StateBackendKind.HTTP)

    settings = load_orchestra_dbt_settings(cwd)
    raw = settings.state_file
    if not raw or not raw.strip():
        return StateBackendConfig(kind=StateBackendKind.HTTP)

    return backend_config_from_state_location(
        raw, resolve_relative_from=project_dir.resolve()
    )


def resolved_state_backend(cwd: Path | None = None) -> StateBackend:
    cfg = resolve_state_backend_config(cwd)
    match cfg.kind:
        case StateBackendKind.HTTP:
            return HttpStateBackend()
        case StateBackendKind.LOCAL_FILE:
            if cfg.local_path is None:
                raise RuntimeError(
                    "State backend config is LOCAL_FILE but local_path is missing"
                )
            return LocalFileStateBackend(cfg.local_path)
        case StateBackendKind.S3:
            from .s3 import S3StateBackend

            if cfg.s3_bucket is None or cfg.s3_key is None:
                raise RuntimeError(
                    "State backend config is S3 but s3_bucket or s3_key is missing"
                )
            return S3StateBackend(cfg.s3_bucket, cfg.s3_key)
