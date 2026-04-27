class StateLoadError(Exception):
    """Raised when state cannot be loaded from the configured backend."""


class StateSaveError(Exception):
    """Raised when state cannot be written to the configured backend."""
