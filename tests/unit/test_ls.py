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
