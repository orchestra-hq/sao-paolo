from importlib.metadata import PackageNotFoundError
from unittest.mock import patch

import pytest

from src.orchestra_dbt.compatibility import is_dbt_v2


@pytest.mark.parametrize(
    ("installed", "expected"),
    [
        ({"dbt-core": "1.12.3"}, False),
        ({"dbt": "2.0.6"}, True),
        ({"dbt-oss": "2.0.5"}, True),
        ({}, False),
    ],
)
def test_is_dbt_v2_reads_the_installed_distribution(installed, expected):
    def fake_version(distribution):
        if distribution not in installed:
            raise PackageNotFoundError(distribution)
        return installed[distribution]

    with patch("src.orchestra_dbt.compatibility.version", side_effect=fake_version):
        assert is_dbt_v2() is expected
