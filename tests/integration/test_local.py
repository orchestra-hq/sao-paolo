import json
from typing import cast
from unittest.mock import patch

from orchestra_dbt.logger import log_reused_models
from orchestra_dbt.sao import calculate_models_to_run
from src.orchestra_dbt.dag import construct_dag
from src.orchestra_dbt.models import (
    Freshness,
    ModelNode,
    NodeType,
    SourceFreshness,
    StateApiModel,
)


@patch("src.orchestra_dbt.dag.get_integration_account_id_from_env")
def test_e2e(mock_get_integration_account_id_from_env):
    mock_get_integration_account_id_from_env.return_value = "TO_BE_COMPLETED"

    local_state = open("local_state.json", "r").read()
    parsed_dag = construct_dag(
        source_freshness=SourceFreshness(sources={}),
        state=StateApiModel.model_validate(json.loads(local_state)),
        manifest_override="local_manifest.json",
    )

    # check that each node defined in edges is in nodes
    for edge in parsed_dag.edges:
        if edge.from_ not in parsed_dag.nodes:
            raise ValueError(f"Node {edge.from_} not found in nodes")
        if edge.to_ not in parsed_dag.nodes:
            raise ValueError(f"Node {edge.to_} not found in nodes")

    calculate_models_to_run(parsed_dag)

    model_paths_to_run = []
    models_to_reuse: dict[str, ModelNode] = {}
    models_count = 0
    for node_id, node in parsed_dag.nodes.items():
        if node.node_type == NodeType.SOURCE:
            continue
        model_node: ModelNode = cast(ModelNode, node)
        if model_paths_to_run and model_node.model_path not in model_paths_to_run:
            continue
        models_count += 1
        if model_node.freshness == Freshness.CLEAN:
            models_to_reuse[node_id] = model_node

    log_reused_models(models_to_reuse)
