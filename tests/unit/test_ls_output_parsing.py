"""How `dbt ls --output json` output becomes paths and selectors.

Kept apart from test_ls.py: exercising get_nodes_to_run means standing in for
dbt itself, where the rest of the ls tests are plain argument building. Real-dbt
coverage of this lives in tests/integration/test_package_only_selection.py.
"""

import json
from unittest.mock import Mock, patch

from src.orchestra_dbt.ls import get_nodes_to_run


def _line(path: str, fqn: list[str]) -> str:
    return json.dumps({"original_file_path": path, "fqn": fqn})


def _nodes_from(lines: list[str]):
    runner = Mock()
    runner.invoke.return_value = Mock(success=True, exception=None, result=lines)
    with patch.dict(
        "sys.modules", {"dbt.cli.main": Mock(dbtRunner=Mock(return_value=runner))}
    ):
        return get_nodes_to_run(())


def test_splits_each_node_into_a_path_and_an_fqn_selector():
    nodes = _nodes_from(
        [
            _line("models/a.sql", ["proj", "a"]),
            _line("seeds/b.csv", ["proj", "nested", "b"]),
        ]
    )

    assert nodes is not None
    assert nodes.paths == ["models/a.sql", "seeds/b.csv"]
    assert nodes.selectors == ["proj.a", "proj.nested.b"]


def test_package_owned_node_keeps_its_package_qualified_fqn():
    """The path is relative to the owning package, so it doesn't resolve against the
    project root -- the fqn is what makes this node selectable."""
    nodes = _nodes_from(
        [_line("models/staging/stg_thing.sql", ["a_package", "staging", "stg_thing"])]
    )

    assert nodes is not None
    assert nodes.paths == ["models/staging/stg_thing.sql"]
    assert nodes.selectors == ["a_package.staging.stg_thing"]


def test_malformed_output_degrades_rather_than_returning_mismatched_lists():
    assert _nodes_from([json.dumps({"fqn": ["proj", "no_path"]})]) is None


def test_no_nodes_gives_empty_lists_not_none():
    nodes = _nodes_from([])

    assert nodes is not None
    assert nodes.paths == []
    assert nodes.selectors == []


def test_dbt_error_output_reaches_the_debug_log_when_parsing_fails(capsys):
    """dbt's stdout is captured so json blobs stay out of the run log. On failure that
    text is the useful part, so it has to reach the debug log rather than be dropped
    -- and it must not leak to stdout on the way."""
    runner = Mock()

    def _invoke(_args):
        print("Encountered an error:\nRuntime Error: something dbt wants to tell you")
        return Mock(success=True, exception=None, result=["not json at all"])

    runner.invoke.side_effect = _invoke
    logged: list[str] = []
    with patch.dict(
        "sys.modules", {"dbt.cli.main": Mock(dbtRunner=Mock(return_value=runner))}
    ):
        with patch(
            "src.orchestra_dbt.ls.log_debug", lambda msg: logged.append(str(msg))
        ):
            assert get_nodes_to_run(()) is None

    assert any("something dbt wants to tell you" in entry for entry in logged)
    assert "something dbt wants to tell you" not in capsys.readouterr().out
