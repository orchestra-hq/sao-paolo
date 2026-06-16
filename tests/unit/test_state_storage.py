import pytest

from src.orchestra_dbt.state_types import parse_gcs_uri, parse_s3_uri


@pytest.mark.parametrize(
    "uri, bucket, key",
    [
        ("s3://my-bucket/path/to/state.json", "my-bucket", "path/to/state.json"),
        ("S3://B/path/k.json", "B", "path/k.json"),
        ("s3://bucket/single", "bucket", "single"),
    ],
)
def test_parse_s3_uri_ok(uri: str, bucket: str, key: str) -> None:
    assert parse_s3_uri(uri) == (bucket, key)


@pytest.mark.parametrize(
    "uri",
    [
        "https://bucket/key",
        "s3://bucket-only",
        "s3://",
        "",
    ],
)
def test_parse_s3_uri_invalid(uri: str) -> None:
    with pytest.raises(ValueError):
        parse_s3_uri(uri)


@pytest.mark.parametrize(
    "uri, bucket, key",
    [
        ("gs://my-bucket/path/to/state.json", "my-bucket", "path/to/state.json"),
        ("GS://B/path/k.json", "B", "path/k.json"),
        ("gs://bucket/single", "bucket", "single"),
    ],
)
def test_parse_gcs_uri_ok(uri: str, bucket: str, key: str) -> None:
    assert parse_gcs_uri(uri) == (bucket, key)


@pytest.mark.parametrize(
    "uri",
    [
        "https://bucket/key",
        "gs://bucket-only",
        "gs://",
        "",
    ],
)
def test_parse_gcs_uri_invalid(uri: str) -> None:
    with pytest.raises(ValueError):
        parse_gcs_uri(uri)


from src.orchestra_dbt.state_backends.factory import resolve_state_backend_config
from src.orchestra_dbt.state_types import StateBackendKind


def test_gs_uri_routes_to_gcs_backend(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.setenv("ORCHESTRA_STATE_FILE", "gs://my-bucket/path/state.json")

    cfg = resolve_state_backend_config()

    assert cfg.kind == StateBackendKind.GCS
    assert cfg.gcs_bucket == "my-bucket"
    assert cfg.gcs_key == "path/state.json"
