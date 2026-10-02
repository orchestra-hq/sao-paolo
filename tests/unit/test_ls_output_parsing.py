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


def _nodes_from(lines: list[str], manifest: dict | None = None):
    runner = Mock()
    runner.invoke.return_value = Mock(success=True, exception=None, result=lines)
    with patch.dict(
        "sys.modules", {"dbt.cli.main": Mock(dbtRunner=Mock(return_value=runner))}
    ):
        if manifest is None:
            return get_nodes_to_run(())
        with patch("src.orchestra_dbt.ls.load_json", return_value=manifest):
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


_MANIFEST = {
    "nodes": {
        "model.proj.stg_events": {
            "resource_type": "model",
            "fqn": ["proj", "staging", "stg_events"],
            "original_file_path": "models/staging/stg_events.sql",
        },
        "seed.proj.raw_events": {
            "resource_type": "seed",
            "fqn": ["proj", "raw_events"],
            "original_file_path": "seeds/raw_events.csv",
        },
        # Shares the seed's fqn, as a singular test can with a root model.
        "test.proj.raw_events": {
            "resource_type": "test",
            "fqn": ["proj", "raw_events"],
            "original_file_path": "tests/raw_events.sql",
        },
    }
}


def test_dbt_2_fqns_resolve_to_paths_through_the_manifest():
    """dbt 2.x returns fqns whatever --output says."""
    nodes = _nodes_from(["proj.staging.stg_events", "proj.raw_events"], _MANIFEST)

    assert nodes is not None
    assert nodes.paths == ["models/staging/stg_events.sql", "seeds/raw_events.csv"]
    assert nodes.selectors == ["proj.staging.stg_events", "proj.raw_events"]


def test_dbt_2_unreadable_manifest_degrades_to_none():
    """None, not []: cli treats [] as "no filter"."""
    runner = Mock()
    runner.invoke.return_value = Mock(success=True, exception=None, result=["proj.a"])
    with (
        patch.dict(
            "sys.modules", {"dbt.cli.main": Mock(dbtRunner=Mock(return_value=runner))}
        ),
        patch("src.orchestra_dbt.ls.load_json", side_effect=FileNotFoundError),
    ):
        assert get_nodes_to_run(()) is None
