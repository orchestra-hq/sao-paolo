import json
import os
import re
from datetime import datetime
from pathlib import Path

from .constants import ORCHESTRA_REUSED_NODE
from .logger import log_debug, log_error, log_info, log_warn
from .models import MaterialisationNode
from .utils import load_yaml, save_yaml


def patch_file(
    file_path: Path,
    reason: str,
    freshness: int | None,
    last_updated: datetime | None,
) -> None:
    """
    This function should add the following config to the top of the file:
    ```
    {{
      config(
        tags=[{ORCHESTRA_REUSED_NODE}],
        meta={
          'orchestra_reused_reason': '{reason}',
          'orchestra_freshness': '{freshness}',
          'orchestra_last_updated': '{last_updated}'
        }
      )
    }}
    ```
    """

    meta_dict: dict[str, str | int] = {
        "orchestra_reused_reason": reason.replace("'", "").replace('"', "")
    }
    if freshness:
        meta_dict["orchestra_freshness"] = freshness
    if last_updated:
        meta_dict["orchestra_last_updated"] = last_updated.isoformat()

    # Replace double quotes with single quotes
    meta_config: str = json.dumps(meta_dict).replace('"', "'")

    file_path.write_text(
        f'{{{{ config(tags=["{ORCHESTRA_REUSED_NODE}"], meta={meta_config}) }}}}\n\n'
        + file_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def revert_patch_file(file_path: Path) -> None:
    # This should remove the config added by `patch_file`.
    content = file_path.read_text(encoding="utf-8")

    # Match the config line with meta dict (handles both single and double quotes)
    pattern = (
        re.escape('{{ config(tags=["')
        + re.escape(ORCHESTRA_REUSED_NODE)
        + re.escape('"], meta=')
        + r".*?"
        + re.escape(") }}\n\n")
    )
    content = re.sub(pattern, "", content, count=1)  # Remove only the first occurrence
    file_path.write_text(content, encoding="utf-8")


def _get_sql_files(cwd: Path) -> list[Path]:
    filtered_files: list[Path] = []
    for root, _, files in os.walk(cwd, followlinks=True):
        # Skip .venv directories
        if ".venv" in root:
            continue
        for name in files:
            if name.endswith(".sql"):
                filtered_files.append(Path(root) / name)
    return filtered_files


def patch_sql_files(nodes_to_reuse: dict[str, MaterialisationNode]) -> None:
    cwd = Path(os.getcwd())
    sql_files = _get_sql_files(cwd)

    if not sql_files:
        log_warn("No .sql files found in project directory.")

    file_paths_to_nodes: dict[str, MaterialisationNode] = {
        node.file_path: node for node in nodes_to_reuse.values()
    }

    for file in sql_files:
        relative_path = str(file.relative_to(cwd))
        if relative_path in file_paths_to_nodes:
            node: MaterialisationNode = file_paths_to_nodes[relative_path]
            try:
                log_debug(f"Patching {relative_path}...")
                patch_file(
                    file_path=file,
                    reason=node.reason,
                    freshness=node.freshness_config.minutes_sla,
                    last_updated=node.last_updated,
                )
            except Exception as e:
                log_warn(f"Failed to add tag to {file}: {e}")


def revert_patching(file_paths_to_revert: list[str]) -> None:
    cwd = Path(os.getcwd())
    sql_files = _get_sql_files(cwd)

    for file in sql_files:
        relative_path = str(file.relative_to(cwd))
        if relative_path in file_paths_to_revert:
            try:
                revert_patch_file(file_path=file)
            except Exception as e:
                log_warn(f"Failed to reset tag from {file}: {e}")


def patch_seed_properties(
    nodes_to_reuse: dict[str, MaterialisationNode],
    seed_properties_file_path: str = "seeds/properties.yml",
) -> None:
    seeds_to_reuse: dict[str, MaterialisationNode] = {
        node.file_path.split("/")[-1].removesuffix(".csv"): node
        for node in nodes_to_reuse.values()
        if node.file_path.endswith(".csv")
    }

    if not seeds_to_reuse:
        return

    try:
        seeds_properties = load_yaml(seed_properties_file_path)
        if "seeds" not in seeds_properties:
            raise ValueError("Missing 'seeds' key. Skipping patching of seeds.")
        if not isinstance(seeds_properties["seeds"], list):
            raise ValueError("'seeds' key must be a list. Skipping patching of seeds.")
    except FileNotFoundError:
        log_info(
            f"No {seed_properties_file_path} file found in project directory. Creating one."
        )
        # If there is no 'seeds' directory, create one.
        os.makedirs("seeds", exist_ok=True)
        seeds_properties: dict = {"seeds": []}
    except Exception as e:
        log_error(f"Error loading {seed_properties_file_path}: {e}")
        return

    for seed_properties in seeds_properties.get("seeds", []):
        seed_name = seed_properties.get("name")
        if not seed_name or seed_name not in seeds_to_reuse:
            continue

        seed_node: MaterialisationNode = seeds_to_reuse.pop(seed_name)

        try:
            existing_tags_on_seed = seed_properties["config"].get("tags", [])
            seed_properties["config"]["tags"] = existing_tags_on_seed + [
                ORCHESTRA_REUSED_NODE
            ]
            existing_meta_on_seed = seed_properties["config"].get("meta", {})
            seed_last_updated = seed_node.last_updated
            seed_properties["config"]["meta"] = existing_meta_on_seed | {
                "orchestra_reused_reason": seed_node.reason,
                "orchestra_last_updated": (
                    seed_last_updated.isoformat() if seed_last_updated else None
                ),
            }
        except KeyError as key_error:
            log_error(
                f"Error updating seed '{seed_name}' properties. Missing key: {key_error}"
            )
        except Exception as e:
            log_error(f"Unknown error updating seed '{seed_name}' properties: {e}")

    for seed_name, node in seeds_to_reuse.items():
        seeds_properties["seeds"].append(
            {
                "name": seed_name,
                "config": {
                    "tags": [ORCHESTRA_REUSED_NODE],
                    "meta": {
                        "orchestra_reused_reason": node.reason,
                        "orchestra_last_updated": node.last_updated.isoformat()
                        if node.last_updated
                        else None,
                    },
                },
            }
        )

    try:
        save_yaml(seed_properties_file_path, seeds_properties)
        log_info(f"Patched {seed_properties_file_path}")
    except Exception as e:
        log_error(f"Error saving {seed_properties_file_path}: {e}")
