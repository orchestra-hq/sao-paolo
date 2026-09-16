import json

from src.orchestra_dbt.ls import get_args_for_ls, parse_ls_output


class TestGetArgsForLs:
    def test_requests_both_path_and_fqn_in_one_invocation(self):
        """Paths and fqn selectors feed different consumers, but re-parsing a large
        project just to get the other form costs tens of seconds -- so ask for both
        at once."""
        args = get_args_for_ls(())

        assert args[-6:] == [
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


class TestParseLsOutput:
    def _line(self, path: str, fqn: list[str]) -> str:
        return json.dumps({"original_file_path": path, "fqn": fqn})

    def test_splits_each_node_into_a_path_and_an_fqn_selector(self):
        nodes = parse_ls_output(
            [
                self._line("models/a.sql", ["proj", "a"]),
                self._line("seeds/b.csv", ["proj", "nested", "b"]),
            ]
        )

        assert nodes.paths == ["models/a.sql", "seeds/b.csv"]
        assert nodes.selectors == ["proj.a", "proj.nested.b"]

    def test_package_owned_nodes_keep_their_package_qualified_fqn(self):
        """A node from an installed package reports an original_file_path relative to
        that package (`models/staging/stg_thing.sql`), while the file actually lives
        at `dbt_packages/a_package/models/staging/stg_thing.sql`. The path is
        therefore useless for selection -- dbt resolves `path:` by globbing the real
        filesystem from the project root -- but the fqn is package-qualified and
        selects correctly."""
        nodes = parse_ls_output(
            [
                self._line(
                    "models/staging/stg_thing.sql",
                    ["a_package", "staging", "stg_thing"],
                )
            ]
        )

        assert nodes.paths == ["models/staging/stg_thing.sql"]
        assert nodes.selectors == ["a_package.staging.stg_thing"]

    def test_tolerates_a_node_missing_either_key(self):
        nodes = parse_ls_output(
            [
                json.dumps({"fqn": ["proj", "only_fqn"]}),
                json.dumps({"original_file_path": "models/only_path.sql"}),
            ]
        )

        assert nodes.paths == ["models/only_path.sql"]
        assert nodes.selectors == ["proj.only_fqn"]

    def test_no_nodes_gives_empty_lists_not_none(self):
        nodes = parse_ls_output([])

        assert nodes.paths == []
        assert nodes.selectors == []
