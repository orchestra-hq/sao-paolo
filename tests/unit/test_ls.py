import json

from src.orchestra_dbt.ls import get_args_for_ls, get_nodes_to_run


class TestGetArgsForLs:
    def test_requests_both_path_and_fqn_in_one_invocation(self):
        """Asking twice would re-resolve the same selection."""
        assert get_args_for_ls(())[-6:] == [
            "--output",
            "json",
            "--output-keys",
            "original_file_path",
            "fqn",
            "-q",
        ]

    def test_forwards_user_args_and_drops_unsupported_ones(self):
        args = get_args_for_ls(("--select", "tag:nightly", "--empty"))

        assert "--select" in args
        assert "tag:nightly" in args
        assert "--empty" not in args


class TestGetNodesToRun:
    def _invoke(self, lines: list[str]):
        from unittest.mock import Mock, patch

        runner = Mock()
        runner.invoke.return_value = Mock(success=True, exception=None, result=lines)
        with patch.dict(
            "sys.modules",
            {"dbt.cli.main": Mock(dbtRunner=Mock(return_value=runner))},
        ):
            return get_nodes_to_run(())

    def _line(self, path: str, fqn: list[str]) -> str:
        return json.dumps({"original_file_path": path, "fqn": fqn})

    def test_splits_each_node_into_a_path_and_an_fqn_selector(self):
        nodes = self._invoke(
            [
                self._line("models/a.sql", ["proj", "a"]),
                self._line("seeds/b.csv", ["proj", "nested", "b"]),
            ]
        )

        assert nodes is not None
        assert nodes.paths == ["models/a.sql", "seeds/b.csv"]
        assert nodes.selectors == ["proj.a", "proj.nested.b"]

    def test_package_owned_node_keeps_its_package_qualified_fqn(self):
        """The path is relative to the owning package, so it doesn't resolve against
        the project root -- the fqn is what makes this node selectable."""
        nodes = self._invoke(
            [
                self._line(
                    "models/staging/stg_thing.sql",
                    ["a_package", "staging", "stg_thing"],
                )
            ]
        )

        assert nodes is not None
        assert nodes.paths == ["models/staging/stg_thing.sql"]
        assert nodes.selectors == ["a_package.staging.stg_thing"]

    def test_malformed_output_degrades_rather_than_returning_mismatched_lists(self):
        assert self._invoke([json.dumps({"fqn": ["proj", "no_path"]})]) is None

    def test_no_nodes_gives_empty_lists_not_none(self):
        nodes = self._invoke([])

        assert nodes is not None
        assert nodes.paths == []
        assert nodes.selectors == []
