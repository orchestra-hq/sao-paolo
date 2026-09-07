import threading
from datetime import datetime

from ..compatibility import dbt_core_import_error_message
from ..logger import log_error, log_info, log_warn
from ..models import SourceFreshness
from ..target_finder import find_target_in_args
from ..utils import load_json
from .fallbacks.registry import FALLBACK_BY_ADAPTER_TYPE, loaded_at_fields_unset


def get_args_for_source_freshness(user_args: tuple | list[str]) -> list[str]:
    args: list[str] = ["source", "freshness", "-q"]
    target = find_target_in_args(list(user_args))
    if target:
        args.extend(["--target", target])
    return args


def get_upstream_source_ids(
    paths: list[str], manifest_override: str | None = None
) -> set[str]:
    """Sources upstream of the given file paths, via dbt's own `parent_map`.

    `paths` is the same `dbt ls`-resolved selection already used elsewhere for
    node reuse (`get_paths_to_run`), and `target/manifest.json` is already on disk
    from that same invocation -- so this just walks a graph dbt already built,
    rather than re-parsing or re-resolving the CLI selection ourselves.
    """
    manifest = load_json(manifest_override or "target/manifest.json")
    parent_map: dict[str, list[str]] = manifest.get("parent_map", {})
    path_to_id = {
        str(node["original_file_path"]): node_id
        for node_id, node in manifest.get("nodes", {}).items()
    }

    upstream_source_ids: set[str] = set()
    seen: set[str] = set()
    queue = [path_to_id[path] for path in paths if path in path_to_id]
    while queue:
        node_id = queue.pop()
        if node_id in seen:
            continue
        seen.add(node_id)
        if node_id.startswith("source."):
            upstream_source_ids.add(node_id)
        queue.extend(parent_map.get(node_id, []))
    return upstream_source_ids


def should_exclude_source(
    compiled_node, require_explicit_source_freshness: bool
) -> bool:
    return require_explicit_source_freshness and loaded_at_fields_unset(compiled_node)


def should_exclude_out_of_scope_source(
    compiled_node, sources_in_scope: set[str] | None
) -> bool:
    return (
        sources_in_scope is not None and compiled_node.unique_id not in sources_in_scope
    )


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

    sources_in_scope: set[str] | None = None
    if scope_to_selection and paths_to_run:
        sources_in_scope = get_upstream_source_ids(paths_to_run)

    sources_without_explicit_freshness: set[str] = set()
    sources_out_of_scope: set[str] = set()

    class OrchestraFreshnessRunner(FreshnessRunner):
        def execute(self, compiled_node, manifest) -> FreshnessNodeResult:
            # setting config: freshness: null can impact the execute method
            # below. In this case, set it back to the default FreshnessThreshold
            # object.
            if compiled_node.freshness is None:
                compiled_node.freshness = FreshnessThreshold()

            if should_exclude_out_of_scope_source(compiled_node, sources_in_scope):
                sources_out_of_scope.add(compiled_node.unique_id)
                return default_freshness_result(compiled_node)

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
        dbtRunner().invoke(args=get_args_for_source_freshness(user_args))
        if sources_without_explicit_freshness:
            log_warn(
                f"{len(sources_without_explicit_freshness)} source(s) have no explicit freshness "
                "config (loaded_at_field or loaded_at_query) and are excluded from state-aware "
                "orchestration; models depending on them will always run."
            )
        if sources_out_of_scope:
            log_info(
                f"{len(sources_out_of_scope)} source(s) outside the current selection's "
                "ancestors skipped."
            )
        excluded = sources_without_explicit_freshness | sources_out_of_scope
        return SourceFreshness(
            sources={
                source["unique_id"]: source["max_loaded_at"]
                for source in load_json("target/sources.json")["results"]
                if source["unique_id"] not in excluded
            }
        )
    except Exception as e:
        log_warn(f"Error running dbt source freshness: {e}")
