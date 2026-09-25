from datetime import datetime

from src.orchestra_dbt.logger import log_reused_nodes
from src.orchestra_dbt.models import (
    Freshness,
    FreshnessConfig,
    MaterialisationNode,
)


class TestLogReusedNodes:
    def test_happy_path_formatting(self, capsys):
        nodes_to_reuse = {
            "node_1": MaterialisationNode(
                asset_external_id="integration_account_id.model.node_1",
                last_updated=datetime(2026, 1, 1),
                checksum="checksum_1",
                freshness_config=FreshnessConfig(),
                freshness=Freshness.CLEAN,
                dbt_path="dbt_path_1",
                reason="reason_1",
                sources={},
                file_path="file_path_1",
            ),
            "node_2": MaterialisationNode(
                asset_external_id="integration_account_id.model.node_2",
                last_updated=None,
                checksum="checksum_1",
                freshness_config=FreshnessConfig(),
                freshness=Freshness.CLEAN,
                dbt_path="dbt_path_1",
                reason="Brand new node",
                sources={},
                file_path="file_path_1",
            ),
        }
        log_reused_nodes(nodes_to_reuse)
        log_lines = capsys.readouterr().out.strip().split("\n")
        log_lines = [line[26:-4] for line in log_lines]
        assert log_lines == [
            "2 node(s) to be reused:",
            "1 of 2 REUSED node_1 - reason_1 (last updated: 2026-01-01 00:00:00)",
            "2 of 2 REUSED node_2 - Brand new node (last updated: none)",
        ]


class TestLogWhyNotReused:
    """A run that reuses nothing printed no reason at all, because only reused nodes
    log theirs -- exactly the case you need to debug."""

    def _candidates(self):
        from src.orchestra_dbt.models import (
            Freshness,
            FreshnessConfig,
            MaterialisationNode,
        )

        def node(name, reason):
            return MaterialisationNode(
                asset_external_id=name,
                checksum="abc",
                freshness=Freshness.DIRTY,
                dbt_path=f"models/{name}.sql",
                file_path=f"models/{name}.sql",
                reason=reason,
                sources={},
                freshness_config=FreshnessConfig(),
            )

        return {
            "model.a": node("a", "Checksum changed since last run."),
            "model.b": node("b", "Checksum changed since last run."),
            "model.c": node("c", "Model not previously seen in state."),
        }

    def test_groups_unreused_nodes_by_reason(self):
        from unittest.mock import call, patch

        from src.orchestra_dbt.logger import log_why_not_reused

        with patch("src.orchestra_dbt.logger.log_debug") as debug:
            log_why_not_reused(self._candidates(), nodes_to_reuse={})

        assert call("3 node(s) not reused, by reason:") in debug.call_args_list
        assert (
            call("  2 node(s): Checksum changed since last run.")
            in debug.call_args_list
        )
        assert (
            call("  1 node(s): Model not previously seen in state.")
            in debug.call_args_list
        )

    def test_ignores_nodes_that_were_reused(self):
        from unittest.mock import patch

        from src.orchestra_dbt.logger import log_why_not_reused

        candidates = self._candidates()
        reused = {k: candidates[k] for k in ("model.a", "model.b")}
        with patch("src.orchestra_dbt.logger.log_debug") as debug:
            log_why_not_reused(candidates, nodes_to_reuse=reused)

        assert debug.call_args_list[0][0][0] == "1 node(s) not reused, by reason:"
