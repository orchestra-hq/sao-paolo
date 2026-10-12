from pathlib import Path

from ..config import (
    OrchestraDbtSettings,
    get_orchestra_api_key,
    get_orchestra_state_file_env_override,
)
from ..state_types import (
    StateBackendConfig,
    StateBackendKind,
    backend_config_from_state_location,
)
from .base import StateBackend
from .http import HttpStateBackend
from .local_file import LocalFileStateBackend


def resolve_state_backend_config(settings: OrchestraDbtSettings) -> StateBackendConfig:
    if get_orchestra_api_key():
        return StateBackendConfig(kind=StateBackendKind.HTTP)

    env_path = get_orchestra_state_file_env_override()
    if env_path:
        return backend_config_from_state_location(
            env_path, resolve_relative_from=Path.cwd().resolve()
        )

    raw = settings.state_file
    if settings.pyproject_dir is None or not raw or not raw.strip():
        return StateBackendConfig(kind=StateBackendKind.HTTP)

    return backend_config_from_state_location(
        raw, resolve_relative_from=settings.pyproject_dir
    )


def resolved_state_backend(settings: OrchestraDbtSettings) -> StateBackend:
    cfg = resolve_state_backend_config(settings)
    account_id = settings.integration_account_id
    match cfg.kind:
        case StateBackendKind.HTTP:
            return HttpStateBackend(settings.orchestra_env, account_id)
        case StateBackendKind.LOCAL_FILE:
            if cfg.local_path is None:
                raise RuntimeError(
                    "State backend config is LOCAL_FILE but local_path is missing"
                )
            return LocalFileStateBackend(cfg.local_path, account_id)
        case StateBackendKind.S3:
            from .s3 import S3StateBackend

            if cfg.s3_bucket is None or cfg.s3_key is None:
                raise RuntimeError(
                    "State backend config is S3 but s3_bucket or s3_key is missing"
                )
            return S3StateBackend(cfg.s3_bucket, cfg.s3_key, account_id)
        case StateBackendKind.GCS:
            from .gcs import GCSStateBackend

            if cfg.gcs_bucket is None or cfg.gcs_key is None:
                raise RuntimeError(
                    "State backend config is GCS but gcs_bucket or gcs_key is missing"
                )
            return GCSStateBackend(cfg.gcs_bucket, cfg.gcs_key, account_id)
        case StateBackendKind.AZURE:
            from .azure import AzureStateBackend

            if (
                cfg.azure_account is None
                or cfg.azure_container is None
                or cfg.azure_key is None
            ):
                raise RuntimeError(
                    "State backend config is AZURE but azure_account, azure_container, or azure_key is missing"
                )
            return AzureStateBackend(
                cfg.azure_account, cfg.azure_container, cfg.azure_key, account_id
            )
