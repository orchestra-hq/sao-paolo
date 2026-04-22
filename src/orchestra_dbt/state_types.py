from enum import Enum
from pathlib import Path

from pydantic import BaseModel, ConfigDict


class StateBackendKind(str, Enum):
    HTTP = "http"
    LOCAL_FILE = "local_file"
    S3 = "s3"


class StateBackendConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: StateBackendKind
    local_path: Path | None = None
    s3_bucket: str | None = None
    s3_key: str | None = None


def parse_s3_uri(uri: str) -> tuple[str, str]:
    prefix = "s3://"
    if not uri.lower().startswith(prefix):
        msg = f"Expected S3 URI starting with {prefix!r}, got: {uri!r}"
        raise ValueError(msg)
    rest = uri[len(prefix) :]
    if "/" not in rest:
        msg = f"S3 URI must include a key after the bucket: {uri!r}"
        raise ValueError(msg)
    bucket, _, key = rest.partition("/")
    bucket = bucket.strip()
    key = key.lstrip("/")
    if not bucket or not key:
        msg = f"Invalid S3 URI (bucket and key required): {uri!r}"
        raise ValueError(msg)
    return bucket, key


def backend_config_from_state_location(
    raw: str, *, resolve_relative_from: Path
) -> StateBackendConfig:
    stripped = raw.strip()
    if stripped.lower().startswith("s3://"):
        bucket, key = parse_s3_uri(stripped)
        return StateBackendConfig(
            kind=StateBackendKind.S3,
            s3_bucket=bucket,
            s3_key=key,
        )
    p = Path(stripped).expanduser()
    if p.is_absolute():
        local_path = p.resolve()
    else:
        local_path = (resolve_relative_from / p).resolve()
    return StateBackendConfig(kind=StateBackendKind.LOCAL_FILE, local_path=local_path)
