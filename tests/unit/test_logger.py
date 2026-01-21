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
