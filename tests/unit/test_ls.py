import pytest

from src.orchestra_dbt.constants import RESOURCE_TYPES_TO_LS
from unittest.mock import patch

from src.orchestra_dbt.ls import (
    DBT_LS_ARGS_NOT_ACCEPTED,
    NodesToRun,
    _nodes_from_ls_result,
    get_args_for_ls,
)

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


def test_stripped_flags_cover_every_build_only_option_in_installed_dbt():
    """Fails when a dbt bump adds a build/run/test option that ls lacks. dbt 2.x
    has no click command tree to compare against, so this only runs on 1.x."""
    cli = pytest.importorskip("dbt.cli.main").cli
    commands = getattr(cli, "commands", None)
    if commands is None:
        pytest.skip("installed dbt has no click command tree")

    def opts(name: str) -> set[str]:
        return {o for p in commands[name].params for o in (*p.opts, *p.secondary_opts)}

    build_only = (opts("build") | opts("run") | opts("test")) - opts("list")
    assert build_only <= DBT_LS_ARGS_NOT_ACCEPTED


class TestNodesFromLsResult:
    """dbt 1.x honours `--output json`. dbt 2.x applies `--output` to stdout only and
    always hands back dotted fqns, which match no node path -- so left untranslated
    every node is filtered out of the reuse decision and nothing is ever reusable."""

    _MANIFEST = {
        "nodes": {
            "model.proj.stg_events": {
                "fqn": ["proj", "staging", "stg_events"],
                "original_file_path": "models/staging/stg_events.sql",
            },
            "seed.proj.raw_events": {
                "fqn": ["proj", "raw_events"],
                "original_file_path": "seeds/raw_events.csv",
            },
        }
    }

    def test_parses_dbt_1_x_json_lines(self):
        items = [
            '{"original_file_path": "models/staging/stg_events.sql",'
            ' "fqn": ["proj", "staging", "stg_events"]}',
            '{"original_file_path": "seeds/raw_events.csv",'
            ' "fqn": ["proj", "raw_events"]}',
        ]

        with patch("src.orchestra_dbt.ls.load_json") as load_json:
            assert _nodes_from_ls_result(items) == NodesToRun(
                paths=["models/staging/stg_events.sql", "seeds/raw_events.csv"],
                selectors=["proj.staging.stg_events", "proj.raw_events"],
            )
        load_json.assert_not_called()

    def test_resolves_dbt_2_x_fqns_through_the_manifest(self):
        with patch("src.orchestra_dbt.ls.load_json", return_value=self._MANIFEST):
            assert _nodes_from_ls_result(
                ["proj.staging.stg_events", "proj.raw_events"]
            ) == NodesToRun(
                paths=["models/staging/stg_events.sql", "seeds/raw_events.csv"],
                selectors=["proj.staging.stg_events", "proj.raw_events"],
            )

    def test_drops_unresolvable_fqns_from_both_lists(self):
        """Dropping from one list only would leave paths and selectors disagreeing."""
        with patch("src.orchestra_dbt.ls.load_json", return_value=self._MANIFEST):
            assert _nodes_from_ls_result(
                ["proj.staging.stg_events", "proj.gone"]
            ) == NodesToRun(
                paths=["models/staging/stg_events.sql"],
                selectors=["proj.staging.stg_events"],
            )

    def test_no_nodes_selected(self):
        with patch("src.orchestra_dbt.ls.load_json") as load_json:
            assert _nodes_from_ls_result([]) == NodesToRun(paths=[], selectors=[])
        load_json.assert_not_called()
