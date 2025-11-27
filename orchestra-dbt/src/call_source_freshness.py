from dbt.cli.main import dbtRunner
from dbt.contracts.graph.nodes import SourceDefinition
from orchestra_dbt.utils import log_warn


def get_sf_from_dbt():
    # Patching of this runs freshness for all defined sources in the dbt config.
    SourceDefinition.has_freshness = True
    try:
        dbtRunner().invoke(["source", "freshness", "-q"])
    except Exception:
        log_warn("Error running dbt source freshness (a source could be expired).")
