import threading
from datetime import datetime

from ..compatibility import dbt_core_import_error_message
from ..logger import log_error, log_info, log_warn
from ..models import SourceFreshness
from ..target_finder import find_target_in_args
from ..utils import load_json
from .fallbacks.registry import FALLBACK_BY_ADAPTER_TYPE, loaded_at_fields_unset


def get_args_for_source_freshness(
    user_args: tuple | list[str],
    scope_to_selection: bool = False,
    selectors_to_run: list[str] | None = None,
) -> list[str]:
    """Build the `dbt source freshness` CLI args: forwards `--target`, and when
    scoped, an ancestor-expanded `--select` built from `selectors_to_run`.

    Selection is by dotted fqn (`package.dir.name`), NOT by `path:`. dbt's
    PathSelectorMethod resolves `path:` by globbing the real filesystem from the
    project root, so a node owned by an installed package -- whose
    original_file_path is relative to that package, not to the root -- never
    matches anything. In a project whose models all live in packages, every
    `path:` criterion misses, dbt selects zero nodes, and source freshness
    silently comes back empty. fqns are read from the manifest and are
    package-qualified, so they resolve for root- and package-owned nodes alike.
    """
    args: list[str] = ["source", "freshness", "-q"]
    target = find_target_in_args(list(user_args))
    if target:
        args.extend(["--target", target])
    if scope_to_selection and selectors_to_run:
        args.append("--select")
        args.extend(f"+{selector}" for selector in selectors_to_run)
    return args


def should_exclude_source(
    compiled_node, require_explicit_source_freshness: bool
) -> bool:
    return require_explicit_source_freshness and loaded_at_fields_unset(compiled_node)


def get_source_freshness(
    user_args: tuple | list[str],
    require_explicit_source_freshness: bool = False,
    scope_to_selection: bool = False,
    selectors_to_run: list[str] | None = None,
) -> SourceFreshness | None:
    try:
        from dbt.artifacts.resources.v1.components import FreshnessThreshold
        from dbt.artifacts.schemas.freshness import SourceDefinition
        from dbt.artifacts.schemas.freshness.v3.freshness import (
            FreshnessNodeResult,
            SourceFreshnessResult,
        )
        from dbt.artifacts.schemas.results import FreshnessStatus
        from dbt.cli.main import dbtRunner
        from dbt.task.freshness import FreshnessRunner, FreshnessTask
        from dbt_common.exceptions import DbtRuntimeError
    except ImportError as missing_dbt_core_error:
        log_error(dbt_core_import_error_message(missing_dbt_core_error))
        raise missing_dbt_core_error

    def default_freshness_result(compiled_node) -> SourceFreshnessResult:
        return SourceFreshnessResult(
            status=FreshnessStatus.Pass,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            adapter_response={},
            message=None,
            failures=None,
            node=compiled_node,
            max_loaded_at=datetime.now(),
            snapshotted_at=datetime.now(),
            age=0,
        )

    sources_without_explicit_freshness: set[str] = set()

    class OrchestraFreshnessRunner(FreshnessRunner):
        def execute(self, compiled_node, manifest) -> FreshnessNodeResult:
            # setting config: freshness: null can impact the execute method
            # below. In this case, set it back to the default FreshnessThreshold
            # object.
            if compiled_node.freshness is None:
                compiled_node.freshness = FreshnessThreshold()

            if should_exclude_source(compiled_node, require_explicit_source_freshness):
                sources_without_explicit_freshness.add(compiled_node.unique_id)
                return default_freshness_result(compiled_node)

            if loaded_at_fields_unset(compiled_node):
                handler = FALLBACK_BY_ADAPTER_TYPE.get(self.adapter.type())
                if handler:
                    res = handler(self, compiled_node, manifest)
                    if res is not None:
                        return res
                    return default_freshness_result(compiled_node)

            try:
                return super().execute(compiled_node, manifest)
            except DbtRuntimeError as e:
                log_warn(
                    f"Unable to calculate source freshness for {compiled_node.unique_id}: {e}"
                )
            return default_freshness_result(compiled_node)

    log_info("Calculating source freshness")

    SourceDefinition.has_freshness = True  # pyright: ignore[reportAttributeAccessIssue]
    FreshnessTask.get_runner_type = lambda self, _: OrchestraFreshnessRunner

    def run_freshness(scoped: bool) -> list[dict]:
        result = dbtRunner().invoke(
            args=get_args_for_source_freshness(user_args, scoped, selectors_to_run)
        )
        # dbtRunner never raises -- it catches everything and reports via `success`,
        # so without this an internal failure is indistinguishable from a clean run
        # that found nothing.
        if not result.success:
            log_warn(
                f"dbt source freshness did not complete successfully: {result.exception}"
            )
        return load_json("target/sources.json")["results"]

    try:
        results = run_freshness(scope_to_selection)

        if scope_to_selection and selectors_to_run and not results:
            # A selection matching nothing is not an error to dbt: it warns
            # "Nothing to do" (swallowed by our -q) and still writes a valid,
            # empty sources.json. Without this fallback that is silently
            # indistinguishable from "this project has no sources", and every
            # downstream model loses its freshness signal.
            log_warn(
                "Scoped source freshness matched no sources. Falling back to an "
                "unscoped run so freshness is still collected."
            )
            results = run_freshness(False)

        if sources_without_explicit_freshness:
            log_warn(
                f"{len(sources_without_explicit_freshness)} source(s) have no explicit freshness "
                "config (loaded_at_field or loaded_at_query) and are excluded from state-aware "
                "orchestration; models depending on them will always run."
            )
        return SourceFreshness(
            sources={
                source["unique_id"]: source["max_loaded_at"]
                for source in results
                if source["unique_id"] not in sources_without_explicit_freshness
            }
        )
    except Exception as e:
        log_warn(f"Error running dbt source freshness: {e}")
