import pytest

from src.orchestra_dbt.state_backends.factory import resolve_state_backend_config
from src.orchestra_dbt.state_types import (
    StateBackendKind,
    parse_abfs_uri,
    parse_gcs_uri,
    parse_s3_uri,
)


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


@pytest.mark.parametrize(
    "uri, account, container, key",
    [
        (
            "abfss://mycontainer@myaccount.dfs.core.windows.net/state.json",
            "myaccount",
            "mycontainer",
            "state.json",
        ),
        (
            "abfs://mycontainer@myaccount.dfs.core.windows.net/state.json",
            "myaccount",
            "mycontainer",
            "state.json",
        ),
        (
            "ABFSS://mycontainer@myaccount.dfs.core.windows.net/path/to/state.json",
            "myaccount",
            "mycontainer",
            "path/to/state.json",
        ),
        (
            "ABFS://mycontainer@myaccount.dfs.core.windows.net/path/to/state.json",
            "myaccount",
            "mycontainer",
            "path/to/state.json",
        ),
        (
            "abfss://mycontainer@myaccount.dfs.core.windows.net/nested/path/state.json",
            "myaccount",
            "mycontainer",
            "nested/path/state.json",
        ),
    ],
)
def test_parse_abfs_uri_ok(uri: str, account: str, container: str, key: str) -> None:
    assert parse_abfs_uri(uri) == (account, container, key)


@pytest.mark.parametrize(
    "uri",
    [
        "https://myaccount.blob.core.windows.net/container/key",
        "abfss://container-only",
        "abfs://container-only",
        "abfss://",
        "abfs://",
        "abfss://container@account.dfs.core.windows.net",  # missing key
        "",
    ],
)
def test_parse_abfs_uri_invalid(uri: str) -> None:
    with pytest.raises(ValueError):
        parse_abfs_uri(uri)


def test_abfss_uri_routes_to_azure_backend(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.setenv(
        "ORCHESTRA_STATE_FILE",
        "abfss://mycontainer@myaccount.dfs.core.windows.net/path/state.json",
    )

    cfg = resolve_state_backend_config()

    assert cfg.kind == StateBackendKind.AZURE
    assert cfg.azure_account == "myaccount"
    assert cfg.azure_container == "mycontainer"
    assert cfg.azure_key == "path/state.json"


def test_abfs_uri_routes_to_azure_backend(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ORCHESTRA_API_KEY", raising=False)
    monkeypatch.setenv(
        "ORCHESTRA_STATE_FILE",
        "abfs://mycontainer@myaccount.dfs.core.windows.net/path/state.json",
    )

    cfg = resolve_state_backend_config()

    assert cfg.kind == StateBackendKind.AZURE
    assert cfg.azure_account == "myaccount"
    assert cfg.azure_container == "mycontainer"
    assert cfg.azure_key == "path/state.json"
