import pytest

from src.orchestra_dbt.asset_external_id import generate_asset_external_id


class TestGenerateAssetExternalId:
    @pytest.mark.parametrize(
        argnames="node_id, node, integration_account_id, expected",
        argvalues=[
            (
                "model.a",
                {"relation_name": "a.b.c"},
                None,
                "model.a",
            ),
            (
                "model.a",
                {},
                "integration_account_id",
                "integration_account_id.model.a",
            ),
            (
                "model.a",
                {"relation_name": "a.b.c"},
                "integration_account_id",
                "integration_account_id.a.b.c",
            ),
        ],
    )
    def test_generate_asset_external_id(
        self, node_id: str, node: dict, integration_account_id: str, expected: str
    ):
        assert (
            generate_asset_external_id(node_id, node, integration_account_id)
            == expected
        )
