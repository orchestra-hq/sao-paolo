import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from src.orchestra_dbt.models import SourceFreshness
from src.orchestra_dbt.source_freshness import (
    get_args_for_source_freshness,
    get_source_freshness,
    get_upstream_source_ids,
    should_exclude_out_of_scope_source,
)


class TestGetArgsForSourceFreshness:
    def test_forwards_target_when_present(self):
        user_args = ("--target", "prod", "--select", "my_model")

        assert get_args_for_source_freshness(user_args) == [
            "source",
            "freshness",
            "-q",
            "--target",
            "prod",
        ]

    def test_omits_target_when_absent(self):
        assert get_args_for_source_freshness(("--select", "my_model")) == [
            "source",
            "freshness",
            "-q",
        ]


# model_a -> source X directly; model_b -> intermediate -> source Y (transitive);
# model_c -> source X too, shared with model_a (diamond); model_d is unrelated.
FAKE_MANIFEST = {
    "nodes": {
        "model.proj.a": {"original_file_path": "models/a.sql"},
        "model.proj.b": {"original_file_path": "models/b.sql"},
        "model.proj.intermediate": {"original_file_path": "models/intermediate.sql"},
        "model.proj.c": {"original_file_path": "models/c.sql"},
        "model.proj.d": {"original_file_path": "models/d.sql"},
    },
    "parent_map": {
        "model.proj.a": ["source.proj.raw.x"],
        "model.proj.b": ["model.proj.intermediate"],
        "model.proj.intermediate": ["source.proj.raw.y"],
        "model.proj.c": ["source.proj.raw.x"],
        "model.proj.d": [],
        "source.proj.raw.x": [],
        "source.proj.raw.y": [],
    },
}


class TestGetUpstreamSourceIds:
    def _write_manifest(self, tmp_path: Path) -> str:
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(FAKE_MANIFEST))
        return str(manifest_path)

    def test_returns_direct_source_parent(self, tmp_path: Path):
        manifest = self._write_manifest(tmp_path)
        assert get_upstream_source_ids(["models/a.sql"], manifest) == {
            "source.proj.raw.x"
        }

    def test_walks_transitive_ancestors(self, tmp_path: Path):
        manifest = self._write_manifest(tmp_path)
        assert get_upstream_source_ids(["models/b.sql"], manifest) == {
            "source.proj.raw.y"
        }

    def test_deduplicates_shared_source_across_paths(self, tmp_path: Path):
        manifest = self._write_manifest(tmp_path)
        assert get_upstream_source_ids(["models/a.sql", "models/c.sql"], manifest) == {
            "source.proj.raw.x"
        }

    def test_unions_across_multiple_unrelated_paths(self, tmp_path: Path):
        manifest = self._write_manifest(tmp_path)
        assert get_upstream_source_ids(["models/a.sql", "models/b.sql"], manifest) == {
            "source.proj.raw.x",
            "source.proj.raw.y",
        }

    def test_path_with_no_source_ancestor_returns_empty(self, tmp_path: Path):
        manifest = self._write_manifest(tmp_path)
        assert get_upstream_source_ids(["models/d.sql"], manifest) == set()

    def test_unknown_path_is_ignored(self, tmp_path: Path):
        manifest = self._write_manifest(tmp_path)
        assert get_upstream_source_ids(["models/nonexistent.sql"], manifest) == set()


class TestShouldExcludeOutOfScopeSource:
    @pytest.mark.parametrize(
        ("unique_id", "sources_in_scope", "expected"),
        [
            ("source.proj.raw.x", None, False),
            ("source.proj.raw.x", {"source.proj.raw.x"}, False),
            ("source.proj.raw.x", {"source.proj.raw.y"}, True),
            ("source.proj.raw.x", set(), True),
        ],
    )
    def test_should_exclude_out_of_scope_source(
        self, unique_id: str, sources_in_scope: set | None, expected: bool
    ):
        node = SimpleNamespace(unique_id=unique_id)
        assert should_exclude_out_of_scope_source(node, sources_in_scope) is expected


class TestGetSourceFreshness:
    def _patched_dbt_modules(self, mock_runner_factory):
        return {
            "dbt.artifacts.resources.v1.components": Mock(FreshnessThreshold=object),
            "dbt.artifacts.schemas.freshness": Mock(
                SourceDefinition=type("SourceDefinition", (), {"has_freshness": False})
            ),
            "dbt.artifacts.schemas.freshness.v3.freshness": Mock(
                SourceFreshnessResult=object
            ),
            "dbt.artifacts.schemas.results": Mock(
                FreshnessStatus=type("FreshnessStatus", (), {"Pass": "pass"})
            ),
            "dbt.cli.main": Mock(dbtRunner=mock_runner_factory),
            "dbt.task.freshness": Mock(
                FreshnessRunner=type("FreshnessRunner", (), {}),
                FreshnessTask=type("FreshnessTask", (), {}),
            ),
            "dbt_common.exceptions": Mock(DbtRuntimeError=Exception),
        }

    def test_default_checks_every_source_and_ignores_selection(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        freshness_result = {
            "results": [
                {
                    "unique_id": "source.proj.raw.x",
                    "max_loaded_at": datetime(2026, 3, 31),
                },
                {
                    "unique_id": "source.proj.raw.y",
                    "max_loaded_at": datetime(2026, 3, 30),
                },
            ]
        }

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value=freshness_result,
            ):
                result = get_source_freshness(
                    ("--target", "prod"), paths_to_run=["models/a.sql"]
                )

        # scope_to_selection defaults to False: the CLI invocation is untouched by
        # paths_to_run, and neither source is excluded from the result.
        mock_runner.invoke.assert_called_once_with(
            args=["source", "freshness", "-q", "--target", "prod"]
        )
        assert result == SourceFreshness(
            sources={
                "source.proj.raw.x": datetime(2026, 3, 31),
                "source.proj.raw.y": datetime(2026, 3, 30),
            }
        )

    def test_scoped_computes_sources_in_scope_from_paths_to_run(self):
        """The dbt invocation itself is unaffected by scoping -- the CLI selection is
        never rewritten. What `scope_to_selection` controls is whether
        `get_upstream_source_ids` runs at all, feeding the in-runner exclusion
        (`should_exclude_out_of_scope_source`, unit-tested above) instead.
        """
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value={"results": []},
            ):
                with patch(
                    "src.orchestra_dbt.source_freshness.get_upstream_source_ids",
                    return_value={"source.proj.raw.x"},
                ) as mock_get_upstream:
                    get_source_freshness(
                        ("--target", "prod"),
                        scope_to_selection=True,
                        paths_to_run=["models/a.sql"],
                    )

        mock_get_upstream.assert_called_once_with(["models/a.sql"])
        mock_runner.invoke.assert_called_once_with(
            args=["source", "freshness", "-q", "--target", "prod"]
        )

    def test_unscoped_never_computes_sources_in_scope(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value={"results": []},
            ):
                with patch(
                    "src.orchestra_dbt.source_freshness.get_upstream_source_ids"
                ) as mock_get_upstream:
                    get_source_freshness((), paths_to_run=["models/a.sql"])

        mock_get_upstream.assert_not_called()

    def test_scoped_with_no_paths_to_run_falls_back_to_unscoped(self):
        mock_runner = Mock()
        mock_runner.invoke.return_value = None
        mock_runner_factory = Mock(return_value=mock_runner)

        with patch.dict("sys.modules", self._patched_dbt_modules(mock_runner_factory)):
            with patch(
                "src.orchestra_dbt.source_freshness.load_json",
                return_value={"results": []},
            ):
                with patch(
                    "src.orchestra_dbt.source_freshness.get_upstream_source_ids"
                ) as mock_get_upstream:
                    get_source_freshness((), scope_to_selection=True, paths_to_run=None)

        mock_get_upstream.assert_not_called()
