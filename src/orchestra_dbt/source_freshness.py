from dbt.cli.main import dbtRunner
from dbt.contracts.graph.nodes import SourceDefinition

from orchestra_dbt.models import SourceFreshness

from .utils import load_file, log_info, log_warn


def get_source_freshness() -> SourceFreshness | None:
    log_info("Calculating source freshness")

    # Patching of this runs freshness for all defined sources in the dbt config.
    SourceDefinition.has_freshness = True  # pyright: ignore[reportAttributeAccessIssue]

    try:
        dbtRunner().invoke(["source", "freshness", "-q"])
        return SourceFreshness(
            sources={
                source["unique_id"]: source["max_loaded_at"]
                for source in load_file("target/sources.json")["results"]
            }
        )
    except Exception as e:
        log_warn(f"Error running dbt source freshness (a source could be expired). {e}")
