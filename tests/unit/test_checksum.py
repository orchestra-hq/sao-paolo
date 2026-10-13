from unittest.mock import patch

from src.orchestra_dbt.checksum import calculate_checksum


class TestCalculateChecksum:
    def test_calculate_checksum_model_matches_dbt_v1_on_any_engine(self):
        """The tutorial's `stg_events`: dbt 1.11 checksums it as below, dbt 2.0 as
        `0dce794b...`, and both manifests carry the same `raw_code`."""
        raw_code = (
            "select\n    id,\n    amount,\n    event_at,\n    customer_id\n"
            "from {{ ref('raw_events') }}"
        )
        assert (
            calculate_checksum(
                resource_type="model",
                node_checksum="0dce794bbc692bdc175e8b942a9dce825d15c16631f3bd76c836ed032eae04c1",
                file_path="models/staging/stg_events.sql",
                raw_code=raw_code + "\n",
            )
            == "a3286f3de1c27c8f4289109a8c85ee5c8f757e77e268541020503fb45975bfae"
        )

    def test_calculate_checksum_snapshot_uses_node_checksum(self):
        assert (
            calculate_checksum(
                resource_type="snapshot",
                node_checksum="123",
                file_path="test.csv",
                raw_code="",
            )
            == "123"
        )

    def test_calculate_checksum_seed_file_not_found(self, capsys):
        assert (
            calculate_checksum(
                resource_type="seed",
                node_checksum="123",
                file_path="test.csv",
                raw_code="",
            )
            is None
        )
        assert (
            "Seed file test.csv not found. Cannot check state for this node."
            in capsys.readouterr().out
        )

    @patch("src.orchestra_dbt.checksum.load_seed_bytes")
    @patch("src.orchestra_dbt.checksum.getsize")
    def test_calculate_checksum_seed(self, mock_getsize, mock_load_seed_bytes):
        mock_getsize.return_value = 1024
        mock_load_seed_bytes.return_value = (
            b"seed_id,seed_value\n1,alpha\n2,beta\n3,gamma"
        )
        assert (
            calculate_checksum(
                resource_type="seed",
                node_checksum="123",
                file_path="test.csv",
                raw_code="",
            )
            == "6d6b2c0c1b1207ba1b98ef592df4f2afd93736f602741903a61f7e3614433634"
        )

    @patch("src.orchestra_dbt.checksum.getsize")
    def test_calculate_checksum_seed_over_100mb_skipped(self, mock_getsize, capsys):
        mock_getsize.return_value = (150 * 1024 * 1024) + 1
        assert (
            calculate_checksum(
                resource_type="seed",
                node_checksum="123",
                file_path="large.csv",
                raw_code="",
            )
            is None
        )
        assert (
            "Seed file large.csv (150.00MB) is over 100.00MB. Skipping checksum."
            in capsys.readouterr().out
        )
