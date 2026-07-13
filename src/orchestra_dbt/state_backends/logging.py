from typing import Literal

from ..logger import log_info
from ..models import StateApiModel

StateBackendLabel = Literal["http", "local_file", "s3", "gcs", "azure"]

_LOAD_MESSAGE_LABEL: dict[StateBackendLabel, str] = {
    "http": "Orchestra HTTP",
    "local_file": "local file",
    "s3": "S3",
    "gcs": "GCS",
    "azure": "Azure Blob Storage",
}


def log_state_loaded(backend: StateBackendLabel, state: StateApiModel) -> None:
    label = _LOAD_MESSAGE_LABEL[backend]
    log_info(f"State loaded ({label}). Retrieved {len(state.state)} items.")


def log_state_saved(backend: StateBackendLabel) -> None:
    if backend in ("s3", "gcs", "azure"):
        log_info(f"State saved to {_LOAD_MESSAGE_LABEL[backend]}")
    else:
        log_info("State saved")
