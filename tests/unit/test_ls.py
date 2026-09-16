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
