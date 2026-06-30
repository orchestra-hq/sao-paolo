from enum import Enum
from pathlib import Path

from pydantic import BaseModel, ConfigDict


class StateBackendKind(str, Enum):
    HTTP = "http"
    LOCAL_FILE = "local_file"
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"


class StateBackendConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: StateBackendKind
    local_path: Path | None = None
    s3_bucket: str | None = None
    s3_key: str | None = None
    gcs_bucket: str | None = None
    gcs_key: str | None = None
    azure_account: str | None = None
    azure_container: str | None = None
    azure_key: str | None = None


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


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    prefix = "gs://"
    if not uri.lower().startswith(prefix):
        msg = f"Expected GCS URI starting with {prefix!r}, got: {uri!r}"
        raise ValueError(msg)
    rest = uri[len(prefix) :]
    if "/" not in rest:
        msg = f"GCS URI must include a key after the bucket: {uri!r}"
        raise ValueError(msg)
    bucket, _, key = rest.partition("/")
    bucket = bucket.strip()
    key = key.lstrip("/")
    if not bucket or not key:
        msg = f"Invalid GCS URI (bucket and key required): {uri!r}"
        raise ValueError(msg)
    return bucket, key


def parse_abfs_uri(uri: str) -> tuple[str, str, str]:
    lower = uri.lower()
    if lower.startswith("abfss://"):
        prefix = "abfss://"
    elif lower.startswith("abfs://"):
        prefix = "abfs://"
    else:
        msg = f"Expected ABFS URI starting with 'abfs://' or 'abfss://', got: {uri!r}"
        raise ValueError(msg)
    rest = uri[len(prefix) :]
    # authority is container@account.dfs.core.windows.net
    if "/" not in rest:
        msg = f"ABFS URI must include a path after the authority: {uri!r}"
        raise ValueError(msg)
    authority, _, key = rest.partition("/")
    key = key.lstrip("/")
    if "@" not in authority:
        msg = f"ABFS URI authority must be container@account.dfs.core.windows.net: {uri!r}"
        raise ValueError(msg)
    container, _, host = authority.partition("@")
    account = host.split(".")[0]
    container = container.strip()
    account = account.strip()
    if not container or not account or not key:
        msg = f"Invalid ABFS URI (container, account, and path required): {uri!r}"
        raise ValueError(msg)
    return account, container, key


def backend_config_from_state_location(
    raw: str, resolve_relative_from: Path
) -> StateBackendConfig:
    stripped = raw.strip()
    if stripped.lower().startswith("s3://"):
        bucket, key = parse_s3_uri(stripped)
        return StateBackendConfig(
            kind=StateBackendKind.S3,
            s3_bucket=bucket,
            s3_key=key,
        )
    if stripped.lower().startswith("gs://"):
        bucket, key = parse_gcs_uri(stripped)
        return StateBackendConfig(
            kind=StateBackendKind.GCS,
            gcs_bucket=bucket,
            gcs_key=key,
        )
    if stripped.lower().startswith(("abfs://", "abfss://")):
        account, container, key = parse_abfs_uri(stripped)
        return StateBackendConfig(
            kind=StateBackendKind.AZURE,
            azure_account=account,
            azure_container=container,
            azure_key=key,
        )
    p = Path(stripped).expanduser()
    if p.is_absolute():
        local_path = p.resolve()
    else:
        local_path = (resolve_relative_from / p).resolve()
    return StateBackendConfig(kind=StateBackendKind.LOCAL_FILE, local_path=local_path)
