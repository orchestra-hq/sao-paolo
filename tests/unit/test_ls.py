from src.orchestra_dbt.ls import get_args_for_ls


class TestGetArgsForLs:
    def test_no_user_args(self):
        assert get_args_for_ls(()) == [
            "ls",
            "--resource-type",
            "model",
            "--resource-type",
            "snapshot",
            "--resource-type",
            "seed",
            "--output",
            "path",
            "-q",
        ]

    def test_with_user_args(self):
        assert get_args_for_ls(("--var", "foo=bar")) == [
            "ls",
            "--resource-type",
            "model",
            "--resource-type",
            "snapshot",
            "--resource-type",
            "seed",
            "--var",
            "foo=bar",
            "--output",
            "path",
            "-q",
        ]

    def test_complex_user_args(self):
        assert get_args_for_ls(
            tuple(
                '-s "source:airbyte_raylo_production.*"+ --empty --exclude tag:daily -t sao-test'.split(
                    " "
                )
            )
        ) == [
            "ls",
            "--resource-type",
            "model",
            "--resource-type",
            "snapshot",
            "--resource-type",
            "seed",
            "-s",
            '"source:airbyte_raylo_production.*"+',
            "--exclude",
            "tag:daily",
            "-t",
            "sao-test",
            "--output",
            "path",
            "-q",
        ]
