from unittest.mock import patch

from src.orchestra_dbt.checksum import calculate_checksum


class TestCalculateChecksum:
    def test_calculate_checksum_not_a_seed(self):
        assert (
            calculate_checksum(
                resource_type="model",
                node_checksum="123",
                file_path="test.csv",
            )
            == "123"
        )

    def test_calculate_checksum_seed_file_not_found(self, capsys):
        assert (
            calculate_checksum(
                resource_type="seed",
                node_checksum="123",
                file_path="test.csv",
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
            )
            is None
        )
        assert (
            "Seed file large.csv (150.00MB) is over 100.00MB. Skipping checksum."
            in capsys.readouterr().out
        )
