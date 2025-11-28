from dbt.cli.main import dbtRunner
from dbt.contracts.graph.nodes import SourceDefinition

from .utils import log_info, log_warn


def source_freshness_invoke():
    log_info("Calculating source freshness")

    # Patching of this runs freshness for all defined sources in the dbt config.
    SourceDefinition.has_freshness = True  # pyright: ignore[reportAttributeAccessIssue]
    try:
        dbtRunner().invoke(["source", "freshness", "-q"])
    except Exception:
        # TODO: improve.
        log_warn("Error running dbt source freshness (a source could be expired).")
