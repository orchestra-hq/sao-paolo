import importlib.util

from src.orchestra_dbt.compatibility import is_dbt_v2

# For tests where nothing connects -- dbt only has to *register* an adapter to
# render the profile and parse. Any installed adapter will do, so take whichever
# this environment has (CI's test job has none and skips; tutorial-dbt has postgres).
_PARSE_ONLY_OUTPUTS = {
    "postgres": """
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: postgres
""",
    "snowflake": """
      type: snowflake
      account: fake_account
      user: fake_user
      password: fake_password
      role: fake_role
      database: fake_db
      warehouse: fake_wh
""",
    "databricks": """
      type: databricks
      host: fake.cloud.databricks.com
      http_path: /sql/1.0/warehouses/fake
      token: fake_token
      catalog: fake_catalog
""",
    "duckdb": """
      type: duckdb
      path: ":memory:"
""",
}


def parse_only_output() -> str | None:
    """A profile output (minus schema/threads) for an installed adapter, if any."""
    if is_dbt_v2():
        # dbt 2.x bundles its adapters and has no dbt.adapters package to probe.
        return _PARSE_ONLY_OUTPUTS["duckdb"]
    return next(
        (
            block
            for name, block in _PARSE_ONLY_OUTPUTS.items()
            if importlib.util.find_spec(f"dbt.adapters.{name}")
        ),
        None,
    )
