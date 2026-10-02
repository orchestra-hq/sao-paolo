from src.orchestra_dbt.constants import RESOURCE_TYPES_TO_LS
from src.orchestra_dbt.ls import get_args_for_ls

_RESOURCE_TYPES = [a for rt in RESOURCE_TYPES_TO_LS for a in ("--resource-type", rt)]
_OUTPUT = ["--output", "json", "--output-keys", "original_file_path", "fqn", "-q"]


class TestGetArgsForLs:
    def test_builds_the_whole_arg_list_in_order(self):
        """Pins ordering, not just membership: `--output-keys` is a variadic option
        that swallows following tokens until a `-`-prefixed one, so user args have to
        stay in front of it."""
        assert get_args_for_ls(()) == ["ls"] + _RESOURCE_TYPES + _OUTPUT

    def test_user_args_stay_ahead_of_the_output_flags(self):
        assert get_args_for_ls(("--select", "tag:nightly", "-t", "prod")) == (
            ["ls"]
            + _RESOURCE_TYPES
            + ["--select", "tag:nightly", "-t", "prod"]
            + _OUTPUT
        )

    def test_drops_args_dbt_ls_does_not_accept(self):
        assert get_args_for_ls(
            ("-s", "source:raw.orders+", "--empty", "--exclude", "tag:daily")
        ) == (
            ["ls"]
            + _RESOURCE_TYPES
            + ["-s", "source:raw.orders+", "--exclude", "tag:daily"]
            + _OUTPUT
        )


class TestGetArgsForLsStripsUnacceptedFlags:
    """dbt ls rejects several flags dbt build/run/test accept; forwarding one makes it
    exit with "No such option" and we lose node-path discovery for the whole run."""

    def test_strips_full_refresh_and_short_form(self):
        for flag in ("--full-refresh", "-f"):
            assert get_args_for_ls(("--select", "x", flag)) == [
                "ls",
                *_RESOURCE_TYPES,
                "--select",
                "x",
                *_OUTPUT,
            ]

    def test_strips_value_flags_with_their_value(self):
        assert get_args_for_ls(("--threads", "8", "--select", "x")) == [
            "ls",
            *_RESOURCE_TYPES,
            "--select",
            "x",
            *_OUTPUT,
        ]

    def test_strips_equals_form_without_eating_the_next_arg(self):
        assert get_args_for_ls(("--threads=8", "--select", "x")) == [
            "ls",
            *_RESOURCE_TYPES,
            "--select",
            "x",
            *_OUTPUT,
        ]

    def test_keeps_flags_dbt_ls_accepts(self):
        assert get_args_for_ls(("--defer", "--state", "prod", "--target", "ci")) == [
            "ls",
            *_RESOURCE_TYPES,
            "--defer",
            "--state",
            "prod",
            "--target",
            "ci",
            *_OUTPUT,
        ]
