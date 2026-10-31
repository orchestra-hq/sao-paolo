import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from .durations import parse_lag_tolerance_seconds
from .project_discovery import find_orchestra_dbt_config


class OrchestraDbtSettings(BaseModel):
    model_config = ConfigDict(extra="ignore")

    state_file: str | None = None
    use_stateful: bool = False
    orchestra_env: Literal["app", "stage", "dev"] = "app"
    local_run: bool = True
    debug: bool = False
    integration_account_id: str | None = None
    seed_state_orchestration: bool = False
    require_explicit_source_freshness: bool = False
    verify_relations_exist: bool = False
    scope_source_freshness_to_selection: bool = False
    # dbt State's lag_tolerance for models with no explicit or inherited window
    # (dbt uses "45m"), given as a duration or integer seconds and stored as
    # seconds. Unset keeps the original behaviour: no window.
    default_lag_tolerance: int | None = None
    # Where the settings were read from; a relative state_file resolves against it.
    pyproject_dir: Path | None = None

    @field_validator("orchestra_env", mode="before")
    @classmethod
    def _normalize_orchestra_env(cls, v: object) -> object:
        if isinstance(v, str):
            return v.lower()
        return v

    @field_validator("default_lag_tolerance", mode="before")
    @classmethod
    def _validate_default_lag_tolerance(cls, v: object) -> int | None:
        if v is None:
            return None
        if isinstance(v, int) and not isinstance(v, bool):
            return parse_lag_tolerance_seconds(v)
        value = str(v).strip()
        if not value:
            return None
        if value.isdigit():
            return int(value)
        return parse_lag_tolerance_seconds(value)


def _env_bool(name: str) -> bool | None:
    if name not in os.environ:
        return None
    return os.environ[name].lower() == "true"


def _env_str(name: str) -> str | None:
    if name not in os.environ:
        return None
    stripped = os.environ[name].strip()
    return stripped if stripped else None


def get_orchestra_api_key() -> str | None:
    return _env_str("ORCHESTRA_API_KEY")


def get_orchestra_state_file_env_override() -> str | None:
    return _env_str("ORCHESTRA_STATE_FILE")


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

    debug = _env_bool("ORCHESTRA_DBT_DEBUG")
    if debug is not None:
        settings = settings.model_copy(update={"debug": debug})

    integration = _env_str("ORCHESTRA_INTEGRATION_ACCOUNT_ID")
    if integration is not None:
        settings = settings.model_copy(update={"integration_account_id": integration})

    seed_state = _env_bool("ORCHESTRA_SEED_STATE_ORCHESTRATION")
    if seed_state is not None:
        settings = settings.model_copy(update={"seed_state_orchestration": seed_state})

    require_explicit = _env_bool("ORCHESTRA_REQUIRE_EXPLICIT_SOURCE_FRESHNESS")
    if require_explicit is not None:
        settings = settings.model_copy(
            update={"require_explicit_source_freshness": require_explicit}
        )

    verify_relations_exist = _env_bool("ORCHESTRA_VERIFY_RELATIONS_EXIST")
    if verify_relations_exist is not None:
        settings = settings.model_copy(
            update={"verify_relations_exist": verify_relations_exist}
        )

    scope_to_selection = _env_bool("ORCHESTRA_SCOPE_SOURCE_FRESHNESS_TO_SELECTION")
    if scope_to_selection is not None:
        settings = settings.model_copy(
            update={"scope_source_freshness_to_selection": scope_to_selection}
        )

    default_lag_tolerance = _env_str("ORCHESTRA_DEFAULT_LAG_TOLERANCE")
    if default_lag_tolerance is not None:
        # Validate now: the field stores parsed seconds, not the raw string.
        settings = OrchestraDbtSettings.model_validate(
            {**settings.model_dump(), "default_lag_tolerance": default_lag_tolerance}
        )

    return OrchestraDbtSettings.model_validate(settings.model_dump())


def load_orchestra_dbt_settings(project_dir: Path) -> OrchestraDbtSettings:
    """Settings from the dbt project's pyproject.toml, else the cwd's, with env overrides."""
    found = find_orchestra_dbt_config(project_dir) or find_orchestra_dbt_config(
        Path.cwd()
    )
    pyproject_dir, raw = found or (None, {})
    try:
        settings = _merge_env_overrides(OrchestraDbtSettings.model_validate(raw))
    except ValidationError as exc:
        raise ValueError(f"Invalid [tool.orchestra_dbt] configuration: {exc}") from exc
    return settings.model_copy(update={"pyproject_dir": pyproject_dir})
