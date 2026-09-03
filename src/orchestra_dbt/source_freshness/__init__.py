import re
import threading
from datetime import datetime

from ..compatibility import dbt_core_import_error_message
from ..logger import log_error, log_info, log_warn
from ..models import SourceFreshness
from ..target_finder import find_target_in_args
from ..utils import load_json
from .fallbacks.registry import FALLBACK_BY_ADAPTER_TYPE, loaded_at_fields_unset

# Flags accepted by `dbt build`/`run`/`test` but not by `dbt source freshness` (checked
# against dbt-core's own click definitions). Boolean ones take no value; the rest do and
# must have their value dropped too, or it's left dangling as a bogus positional arg.
INVALID_SOURCE_FRESHNESS_BOOLEAN_FLAGS = {
    "-f",
    "--full-refresh",
    "--empty",
    "--no-empty",
    "--show",
    "--store-failures",
    "--export-saved-queries",
    "--no-export-saved-queries",
    "--include-saved-query",
    "--no-include-saved-query",
}
INVALID_SOURCE_FRESHNESS_VALUE_FLAGS = {
    "--event-time-start",
    "--event-time-end",
    "--resource-type",
    "--resource-types",
    "--exclude-resource-type",
    "--exclude-resource-types",
    "--sample",
}

# Flags whose following bare tokens are node-selection criteria (dbt's MultiOption:
# one flag, then every non-flag token up to the next flag is a separate criterion).
SELECT_FLAGS = {"-s", "--select", "-m", "--models", "--model"}

_LEADING_ANCESTOR_OPERATOR_RE = re.compile(r"^(\d*\+|@)")


def _filter_invalid_source_freshness_args(user_args: tuple | list[str]) -> list[str]:
    filtered: list[str] = []
    skip_value = False
    for arg in user_args:
        if skip_value:
            skip_value = False
            continue
        flag = arg.split("=", 1)[0]
        if flag in INVALID_SOURCE_FRESHNESS_BOOLEAN_FLAGS:
            continue
        if flag in INVALID_SOURCE_FRESHNESS_VALUE_FLAGS:
            skip_value = "=" not in arg
            continue
        filtered.append(arg)
    return filtered


def _scope_selection_to_ancestors(args: list[str]) -> list[str]:
    """Prefix each `--select`/`--models` criterion with `+` so freshness is scoped to
    the sources upstream of the selected nodes, not just the selected nodes themselves.

    dbt's own selection is exact-match by default (`--select my_model` selects only
    `my_model`); reaching anything upstream needs the explicit ancestor operator.
    Criteria that already carry a graph operator (`+`, `N+`, `@`) are left alone.

    `--exclude` is forwarded unchanged: excluding a node's ancestors is a different (and
    ambiguous) operation. `--selector` is also forwarded unchanged -- a named selector
    can't be rewritten from here; give it its own `+` in the YAML definition if it needs
    to reach upstream sources.
    """
    result: list[str] = []
    in_select = False
    for arg in args:
        if arg in SELECT_FLAGS:
            in_select = True
            result.append(arg)
            continue
        if arg.startswith("-"):
            in_select = False
            result.append(arg)
            continue
        if in_select and not _LEADING_ANCESTOR_OPERATOR_RE.match(arg):
            result.append(f"+{arg}")
        else:
            result.append(arg)
    return result


def get_args_for_source_freshness(
    user_args: tuple | list[str], scope_to_selection: bool = False
) -> list[str]:
    """Build the `dbt source freshness` invocation.

    By default (`scope_to_selection=False`) only `--target` is carried over, matching
    dbt's own freshness behaviour of checking every source in the project. When enabled,
    the triggering command's own selection (`--select`/`--models`) is forwarded too,
    expanded to ancestors, so freshness is only checked for sources upstream of what's
    actually being built.
    """
    if not scope_to_selection:
        args: list[str] = ["source", "freshness", "-q"]
        target = find_target_in_args(list(user_args))
        if target:
            args.extend(["--target", target])
        return args

    filtered_user_args = _filter_invalid_source_freshness_args(user_args)
    scoped_args = _scope_selection_to_ancestors(filtered_user_args)
    return ["source", "freshness", "-q"] + scoped_args


def should_exclude_source(
    compiled_node, require_explicit_source_freshness: bool
) -> bool:
    return require_explicit_source_freshness and loaded_at_fields_unset(compiled_node)


def get_source_freshness(
    user_args: tuple | list[str],
    require_explicit_source_freshness: bool = False,
    scope_to_selection: bool = False,
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

    try:
        dbtRunner().invoke(
            args=get_args_for_source_freshness(user_args, scope_to_selection)
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
