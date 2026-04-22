from typing import Literal

from ..logger import log_info
from ..models import StateApiModel


def log_state_loaded(source: str, state: StateApiModel) -> None:
    log_info(f"State loaded ({source}). Retrieved {len(state.state)} items.")


def log_state_saved(
    backend: Literal["http", "local_file", "s3"],
) -> None:
    if backend == "s3":
        log_info("State saved to S3")
    else:
        log_info("State saved")
