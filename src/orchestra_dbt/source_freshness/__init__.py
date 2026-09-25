import threading
from collections import Counter
from datetime import datetime

from ..compatibility import dbt_core_import_error_message
from ..logger import log_debug, log_error, log_info, log_warn
from ..models import SourceFreshness
from ..target_finder import find_target_in_args
from ..utils import load_json
from .fallbacks.registry import FALLBACK_BY_ADAPTER_TYPE, loaded_at_fields_unset


def get_args_for_source_freshness(
    user_args: tuple | list[str],
    scope_to_selection: bool = False,
    paths_to_run: list[str] | None = None,
) -> list[str]:
    """Build the `dbt source freshness` CLI args: forwards `--target`, and when
    scoped, an ancestor-expanded `--select` built from `paths_to_run`."""
    args: list[str] = ["source", "freshness", "-q"]
    target = find_target_in_args(list(user_args))
    if target:
        args.extend(["--target", target])
    if scope_to_selection and paths_to_run:
        args.append("--select")
        args.extend(f"+path:{path}" for path in paths_to_run)
    return args


def should_exclude_source(
    compiled_node, require_explicit_source_freshness: bool
) -> bool:
    return require_explicit_source_freshness and loaded_at_fields_unset(compiled_node)


def _explicit_freshness_source_ids(results: list[dict]) -> set[str] | None:
    """The `unique_id`s whose source sets `loaded_at_field`/`loaded_at_query`, read from
    the manifest.

    dbt 2.x's `sources.json` omits both from each result's `criteria` -- even for a
    source that does set one -- so the freshness artifact alone cannot tell us. Returns
    None when the manifest cannot answer, so the caller can say so rather than treat
    every source as implicit.

    Both live on the source node, and are duplicated under its `config`. Source-level
    inheritance is already resolved into each node, so there is nothing to walk up to.
    """
    try:
        manifest_sources = load_json("target/manifest.json").get("sources") or {}
    except Exception as e:
        log_debug(f"Could not read target/manifest.json: {e}")
        return None
    if not manifest_sources:
        return None

    explicit: set[str] = set()
    for unique_id in (result["unique_id"] for result in results):
        node = manifest_sources.get(unique_id) or {}
        config = node.get("config") or {}
        if any(
            (node.get(key) or config.get(key))
            for key in ("loaded_at_field", "loaded_at_query")
        ):
            explicit.add(unique_id)
    return explicit


def _log_freshness_outcome(result, results: list[dict]) -> None:
    """dbt 2.x reports a stale or failing source as `success=False` with *no* exception
    -- its documented way of saying "handled, and already accounted for in the
    artifact". Only a real engine error carries one, so only that deserves a warning;
    a project with a permanently stale source would otherwise cry wolf every run.
    """
    if result is None or getattr(result, "success", True):
        return

    exception = getattr(result, "exception", None)
    if exception is not None:
        log_warn(
            f"dbt source freshness errored: {exception}. Using whatever results it wrote."
        )
        return

    statuses = Counter(str(source.get("status")) for source in results)
    log_debug(
        f"dbt source freshness exited non-zero (code {getattr(result, 'exit_code', None)}) "
        "because some sources are not fresh: "
        + ", ".join(f"{count} {status}" for status, count in sorted(statuses.items()))
    )


def _get_source_freshness_v2(
    user_args: tuple | list[str],
    require_explicit_source_freshness: bool,
    scope_to_selection: bool,
    paths_to_run: list[str] | None,
) -> SourceFreshness | None:
    """dbt-core >=2.0 (Fusion) moved task execution into its Rust engine and no longer
    exposes `dbt.task.freshness` / `dbt.adapters` for Orchestra to patch in-process.

    This runs dbt's own, unpatched `dbt source freshness` instead. dbt still computes
    freshness from warehouse metadata when a source sets neither `loaded_at_field` nor
    `loaded_at_query`, exactly as on 1.x, so those sources are kept; only Orchestra's
    adapter-specific fallbacks (e.g. Databricks `DESCRIBE HISTORY`) have no 2.x
    equivalent. A source dbt could not resolve at all has no `max_loaded_at` and is
    dropped, so its downstream models run rather than risk a wrong reuse decision.

    A stale source (`status: "Error"`) is kept: its `max_loaded_at` is real -- the data
    genuinely has not moved -- which is exactly the signal reuse needs.
    """
    from dbt.cli.main import dbtRunner

    log_info("Calculating source freshness")
    log_warn(
        "dbt-core 2.x does not expose the Python freshness runner Orchestra patches; "
        "running dbt's native `dbt source freshness` without adapter-specific fallbacks."
    )

    try:
        result = dbtRunner().invoke(
            args=get_args_for_source_freshness(
                user_args, scope_to_selection, paths_to_run
            )
        )
        results = load_json("target/sources.json")["results"]
        _log_freshness_outcome(result, results)
        excluded: set[str] = set()
        if require_explicit_source_freshness:
            explicit = _explicit_freshness_source_ids(results)
            if explicit is None:
                log_warn(
                    "require_explicit_source_freshness is set, but dbt 2.x's sources.json "
                    "does not record loaded_at_field/loaded_at_query and target/manifest.json "
                    "could not be read to recover them. No source is excluded this run."
                )
            else:
                excluded = {
                    source["unique_id"]
                    for source in results
                    if source["unique_id"] not in explicit
                }
        sources = {
            source["unique_id"]: source["max_loaded_at"]
            for source in results
            if source["unique_id"] not in excluded and source.get("max_loaded_at")
        }

        if excluded:
            log_warn(
                f"{len(excluded)} source(s) have no explicit freshness config (loaded_at_field "
                "or loaded_at_query) and are excluded from state-aware orchestration; "
                "models depending on them will always run."
            )
        # Only sources with a `freshness:` block are checked at all; the rest never
        # reach `sources.json`, so they are absent here rather than counted as failures.
        log_debug(f"dbt freshness-checked {len(results)} source(s).")
        unresolved = len(results) - len(excluded) - len(sources)
        if unresolved:
            log_warn(
                f"{unresolved} of the {len(results)} source(s) dbt freshness-checked returned "
                "no max_loaded_at; models depending on them will always run."
            )
        return SourceFreshness(sources=sources)
    except Exception as e:
        log_warn(f"Error running dbt source freshness: {e}")
        return None


def get_source_freshness(
    user_args: tuple | list[str],
    require_explicit_source_freshness: bool = False,
    scope_to_selection: bool = False,
    paths_to_run: list[str] | None = None,
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
        try:
            from dbt.cli.main import dbtRunner  # noqa: F401
        except ImportError:
            log_error(dbt_core_import_error_message(missing_dbt_core_error))
            raise missing_dbt_core_error
        return _get_source_freshness_v2(
            user_args,
            require_explicit_source_freshness,
            scope_to_selection,
            paths_to_run,
        )

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

    try:
        dbtRunner().invoke(
            args=get_args_for_source_freshness(
                user_args, scope_to_selection, paths_to_run
            )
        )
        if sources_without_explicit_freshness:
            log_warn(
                f"{len(sources_without_explicit_freshness)} source(s) have no explicit freshness "
                "config (loaded_at_field or loaded_at_query) and are excluded from state-aware "
                "orchestration; models depending on them will always run."
            )
        return SourceFreshness(
            sources={
                source["unique_id"]: source["max_loaded_at"]
                for source in load_json("target/sources.json")["results"]
                if source["unique_id"] not in sources_without_explicit_freshness
            }
        )
    except Exception as e:
        log_warn(f"Error running dbt source freshness: {e}")
