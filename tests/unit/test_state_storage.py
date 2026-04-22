import pytest

from src.orchestra_dbt.state_types import parse_s3_uri


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
