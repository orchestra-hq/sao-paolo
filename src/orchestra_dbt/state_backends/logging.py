from typing import Literal

from ..logger import log_info
from ..models import StateApiModel

StateBackendLabel = Literal["http", "local_file", "s3"]

_LOAD_MESSAGE_LABEL: dict[StateBackendLabel, str] = {
    "http": "Orchestra HTTP",
    "local_file": "local file",
    "s3": "S3",
}


def log_state_loaded(backend: StateBackendLabel, state: StateApiModel) -> None:
    label = _LOAD_MESSAGE_LABEL[backend]
    log_info(f"State loaded ({label}). Retrieved {len(state.state)} items.")


def log_state_saved(backend: StateBackendLabel) -> None:
    if backend == "s3":
        log_info("State saved to S3")
    else:
        log_info("State saved")
