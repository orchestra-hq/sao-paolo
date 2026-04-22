from typing import Protocol

from ..models import StateApiModel


class StateBackend(Protocol):
    def load(self) -> StateApiModel: ...

    def save(self, state: StateApiModel) -> None: ...
