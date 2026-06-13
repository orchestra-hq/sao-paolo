import uuid
from typing import Any

from .constants import INDIRECT_SELECTION_CAUTIOUS, ORCHESTRA_REUSED_NODE
from .logger import log_error, log_warn
from .utils import load_yaml, save_yaml

_SELECT_FLAGS = frozenset({"--select", "-s", "--models", "--model", "-m"})
_EXCLUDE_FLAGS = frozenset({"--exclude"})
_TEST_RUNNING_SUBCOMMANDS = frozenset({"build", "test"})


def _reused_exclusion_criteria() -> dict[str, Any]:
    return {
        "method": "tag",
        "value": ORCHESTRA_REUSED_NODE,
        "indirect_selection": INDIRECT_SELECTION_CAUTIOUS,
    }


def _get_reused_selector_definition(existing_selector: str) -> dict[str, Any]:
    return {
        "intersection": [
            {"method": "selector", "value": existing_selector},
            {"exclude": [_reused_exclusion_criteria()]},
        ]
    }


def update_selectors_yaml(selector_tag: str) -> bool:
    try:
        selectors_yml = load_yaml("selectors.yml")
    except FileNotFoundError:
        log_error(
            "A `--selector` was used on the command, but no `selectors.yml` file found."
        )
        return False

    selectors = selectors_yml.get("selectors", [])
    if not selectors or not isinstance(selectors, list):
        log_error(
            "A `--selector` was used on the command, but no valid selectors found in `selectors.yml`."
        )
        return False

    existing_selector_index = next(
        (i for i, s in enumerate(selectors) if s.get("name") == selector_tag), None
    )
    if existing_selector_index is None:
        log_error(f"Selector `{selector_tag}` not found in `selectors.yml`.")
        return False

    random_uuid_underscore_selector_tag = str(uuid.uuid4()).replace("-", "_")
    selectors[existing_selector_index]["name"] = random_uuid_underscore_selector_tag
    selectors.append(
        {
            "name": selector_tag,
            "definition": _get_reused_selector_definition(
                random_uuid_underscore_selector_tag
            ),
        }
    )

    try:
        save_yaml("selectors.yml", {"selectors": selectors})
    except Exception as e:
        log_error(f"Error saving selectors.yml: {e}")
        return False

    return True


def _split_selection_args(
    args: list[str],
) -> tuple[list[str], list[str], list[str]]:
    passthrough: list[str] = []
    includes: list[str] = []
    excludes: list[str] = []

    index = 0
    while index < len(args):
        token = args[index]
        flag = token.split("=", 1)[0]
        if flag in _SELECT_FLAGS:
            target = includes
        elif flag in _EXCLUDE_FLAGS:
            target = excludes
        else:
            passthrough.append(token)
            index += 1
            continue

        if "=" in token:
            target.extend(token.split("=", 1)[1].split())
            index += 1
            continue

        index += 1
        while index < len(args) and not args[index].startswith("-"):
            target.extend(args[index].split())
            index += 1

    return passthrough, includes, excludes


def _build_generated_selector_definition(
    includes: list[str], excludes: list[str]
) -> dict[str, Any]:
    union: list[Any] = list(includes) if includes else ["fqn:*"]
    union.append({"exclude": [*excludes, _reused_exclusion_criteria()]})
    return {"union": union}


def _append_generated_selector(definition: dict[str, Any]) -> str | None:
    try:
        selectors_yml = load_yaml("selectors.yml")
    except FileNotFoundError:
        selectors_yml = {"selectors": []}
    except Exception as e:
        log_error(f"Error loading selectors.yml: {e}")
        return None

    selectors = selectors_yml.get("selectors")
    if not isinstance(selectors, list):
        selectors = []

    name = f"orchestra_reused_{str(uuid.uuid4()).replace('-', '_')}"
    selectors.append({"name": name, "definition": definition})

    try:
        save_yaml("selectors.yml", {"selectors": selectors})
    except Exception as e:
        log_error(f"Error saving selectors.yml: {e}")
        return None

    return name


def _command_runs_tests(cmd: list[str]) -> bool:
    return len(cmd) > 1 and cmd[1] in _TEST_RUNNING_SUBCOMMANDS


def _has_indirect_selection_flag(cmd: list[str]) -> bool:
    return any(arg.split("=", 1)[0] == "--indirect-selection" for arg in cmd)


def modify_dbt_command(cmd: list[str]) -> list[str]:
    if "--selector" in cmd:
        success_updating_selectors = False
        try:
            selector_tag = cmd[cmd.index("--selector") + 1]
            success_updating_selectors = update_selectors_yaml(selector_tag)
            if not success_updating_selectors:
                log_warn("dbt will not run in stateful mode.")
        except IndexError:
            log_error(
                "A `--selector` was used on the command, but no selector tag provided."
            )
        return cmd

    passthrough, includes, excludes = _split_selection_args(cmd[2:])
    user_has_selection = bool(includes or excludes)

    if user_has_selection and _command_runs_tests(cmd):
        definition = _build_generated_selector_definition(includes, excludes)
        selector_name = _append_generated_selector(definition)
        if selector_name is not None:
            return [cmd[0], cmd[1], *passthrough, "--selector", selector_name]
        log_warn(
            "Could not write a generated selector; falling back to tag exclusion, "
            "which may skip tests that span reused and freshly-built models."
        )

    cmd += ["--exclude", f"tag:{ORCHESTRA_REUSED_NODE}"]
    if (
        _command_runs_tests(cmd)
        and not user_has_selection
        and not _has_indirect_selection_flag(cmd)
    ):
        cmd += ["--indirect-selection", INDIRECT_SELECTION_CAUTIOUS]

    return cmd
