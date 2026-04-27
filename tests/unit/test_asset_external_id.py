import pytest

from src.orchestra_dbt.asset_external_id import generate_asset_external_id


class TestGenerateAssetExternalId:
    @pytest.mark.parametrize(
        argnames="node_id, relation_name, integration_account_id, local_run, expected",
        argvalues=[
            (
                "model.a",
                "a.b.c",
                None,
                False,
                "model.a",
            ),
            (
                "model.a",
                None,
                "integration_account_id",
                False,
                "integration_account_id.model.a",
            ),
            (
                "model.a",
                "a.b.c",
                "integration_account_id",
                False,
                "integration_account_id.a.b.c",
            ),
            (
                "model.a",
                "a.b.c",
                "integration_account_id",
                True,
                "model.a",
            ),
        ],
    )
    def test_generate_asset_external_id(
        self,
        node_id: str,
        relation_name: str | None,
        integration_account_id: str | None,
        local_run: bool,
        expected: str,
    ):
        assert (
            generate_asset_external_id(
                node_id,
                relation_name,
                integration_account_id,
                local_run=local_run,
            )
            == expected
        )
