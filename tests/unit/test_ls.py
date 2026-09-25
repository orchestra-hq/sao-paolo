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


class TestGetArgsForLsStripsUnacceptedFlags:
    """dbt ls rejects several flags dbt build/run/test accept; forwarding one makes it
    exit with "No such option" and we lose node-path discovery for the whole run."""

    _BASE = [
        "ls",
        "--resource-type",
        "model",
        "--resource-type",
        "snapshot",
        "--resource-type",
        "seed",
    ]
    _TAIL = ["--output", "path", "-q"]

    def test_strips_full_refresh_and_short_form(self):
        for flag in ("--full-refresh", "-f"):
            assert get_args_for_ls(("--select", "x", flag)) == [
                *self._BASE,
                "--select",
                "x",
                *self._TAIL,
            ]

    def test_strips_value_flags_with_their_value(self):
        assert get_args_for_ls(("--threads", "8", "--select", "x")) == [
            *self._BASE,
            "--select",
            "x",
            *self._TAIL,
        ]

    def test_strips_equals_form_without_eating_the_next_arg(self):
        assert get_args_for_ls(("--threads=8", "--select", "x")) == [
            *self._BASE,
            "--select",
            "x",
            *self._TAIL,
        ]

    def test_keeps_flags_dbt_ls_accepts(self):
        assert get_args_for_ls(("--defer", "--state", "prod", "--target", "ci")) == [
            *self._BASE,
            "--defer",
            "--state",
            "prod",
            "--target",
            "ci",
            *self._TAIL,
        ]
