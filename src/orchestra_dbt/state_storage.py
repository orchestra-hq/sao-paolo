from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class StatePersistenceKind(str, Enum):
    HTTP = "http"
    LOCAL_FILE = "local_file"
    S3 = "s3"


@dataclass(frozen=True)
class StatePersistence:
    kind: StatePersistenceKind
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
