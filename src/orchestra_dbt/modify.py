import uuid
from typing import Any

from .constants import ORCHESTRA_REUSED_NODE
from .logger import log_error, log_warn
from .utils import load_yaml, save_yaml


def _get_reused_selector_definition(existing_selector: str) -> dict[str, Any]:
    return {
        "intersection": [
            {"method": "selector", "value": existing_selector},
            {"exclude": [{"method": "tag", "value": ORCHESTRA_REUSED_NODE}]},
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
    else:
        cmd += ["--exclude", f"tag:{ORCHESTRA_REUSED_NODE}"]

    return cmd
