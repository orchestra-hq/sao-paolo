from unittest.mock import patch

from src.orchestra_dbt.ls import _resolve_paths, get_args_for_ls


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


class TestResolvePaths:
    """dbt 2.x honours --output path only on stdout; the programmatic result is always
    fully-qualified names. Left untranslated, nothing matches node.dbt_path and no node
    is ever reusable."""

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

    def test_translates_fully_qualified_names_to_paths(self):
        with patch("src.orchestra_dbt.ls.load_json", return_value=self._MANIFEST):
            assert _resolve_paths(["proj.staging.stg_events", "proj.raw_events"]) == [
                "models/staging/stg_events.sql",
                "seeds/raw_events.csv",
            ]

    def test_leaves_dbt_1_x_paths_untouched(self):
        paths = ["models/staging/stg_events.sql", "seeds/raw_events.csv"]
        with patch("src.orchestra_dbt.ls.load_json") as load_json:
            assert _resolve_paths(paths) == paths
        load_json.assert_not_called()

    def test_skips_names_with_no_manifest_entry(self):
        with patch("src.orchestra_dbt.ls.load_json", return_value=self._MANIFEST):
            assert _resolve_paths(["proj.staging.stg_events", "proj.gone"]) == [
                "models/staging/stg_events.sql"
            ]

    def test_unreadable_manifest_yields_no_paths(self):
        with patch("src.orchestra_dbt.ls.load_json", side_effect=FileNotFoundError):
            assert _resolve_paths(["proj.staging.stg_events"]) == []
