import os
from pathlib import Path

from ..project_discovery import find_pyproject_directory, read_orchestra_dbt_tool_config
from ..state_types import (
    StateBackendConfig,
    StateBackendKind,
    backend_config_from_state_location,
)
from .http import HttpStateBackend
from .local_file import LocalFileStateBackend


def resolve_state_backend_config(cwd: Path | None = None) -> StateBackendConfig:
    if os.getenv("ORCHESTRA_API_KEY", "").strip():
        return StateBackendConfig(kind=StateBackendKind.HTTP)

    base = cwd or Path.cwd()
    env_path = os.getenv("ORCHESTRA_STATE_FILE", "").strip()
    if env_path:
        return backend_config_from_state_location(
            env_path, resolve_relative_from=base.resolve()
        )

    project_dir = find_pyproject_directory(base)
    if project_dir is None:
        return StateBackendConfig(kind=StateBackendKind.HTTP)

    tool_cfg = read_orchestra_dbt_tool_config(project_dir)
    raw = tool_cfg.get("state_file")
    if not raw or not isinstance(raw, str) or not raw.strip():
        return StateBackendConfig(kind=StateBackendKind.HTTP)

    return backend_config_from_state_location(
        raw, resolve_relative_from=project_dir.resolve()
    )


def resolved_state_backend(cwd: Path | None = None):
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
