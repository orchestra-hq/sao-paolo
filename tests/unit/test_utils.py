import os
from unittest.mock import patch

import pytest

from src.orchestra_dbt.utils import validate_environment


class TestValidateEnvironment:
    @pytest.mark.parametrize(
        "osenv",
        [
            {},
            {
                "ORCHESTRA_ENV": "invalid",
                "ORCHESTRA_API_KEY": "test-key",
            },
        ],
    )
    def test_invalid_environment(self, osenv):
        with patch.dict(os.environ, osenv, clear=True):
            with pytest.raises(SystemExit):
                validate_environment()

    def test_validate_environment_success(self):
        with patch.dict(
            os.environ,
            {
                "ORCHESTRA_API_KEY": "test-key",
                "ORCHESTRA_ENV": "app",
            },
            clear=True,
        ):
            validate_environment()
