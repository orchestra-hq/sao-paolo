import json
import os
import tempfile
from pathlib import Path

from pydantic import ValidationError

from ..logger import log_info
from ..models import StateApiModel
from ..state_errors import StateLoadError, StateSaveError
from .logging import log_state_loaded, log_state_saved


class LocalFileStateBackend:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> StateApiModel:
        path = self._path
        if not path.is_file():
            if path.exists():
                raise StateLoadError(f"State path is not a file: {path}")
            log_info(f"No state file at {path}; starting with empty state.")
            return StateApiModel(state={})

        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise StateLoadError(f"State file is not valid JSON ({path}): {e}")

        try:
            state = StateApiModel.model_validate(data)
        except (ValidationError, ValueError) as e:
            raise StateLoadError(f"State file failed validation ({path}): {e}")

        log_state_loaded("local_file", state)
        return state

    def save(self, state: StateApiModel) -> None:
        path = self._path
        path.parent.mkdir(parents=True, exist_ok=True)
        payload_bytes = state.model_dump_json(exclude_none=True).encode("utf-8")
        fd, tmp_path = tempfile.mkstemp(
            dir=path.parent, prefix=".orchestra_state_", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "wb") as tmp_file:
                tmp_file.write(payload_bytes)
            os.replace(tmp_path, path)
        except OSError as e:
            try:
                if os.path.isfile(tmp_path):
                    os.unlink(tmp_path)
            except OSError:
                pass
            raise StateSaveError(f"Failed to save state file ({path}): {e}") from e
        log_state_saved("local_file")
