from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel


class Freshness(str, Enum):
    CLEAN = "CLEAN"
    DIRTY = "DIRTY"


class NodeType(str, Enum):
    MATERIALISATION = "MATERIALISATION"
    SOURCE = "SOURCE"


class StateItem(BaseModel):
    last_updated: datetime
    checksum: str
    sources: dict[str, datetime]


class StateApiModel(BaseModel):
    state: dict[str, StateItem]


class SourceFreshness(BaseModel):
    sources: dict[str, datetime]


class FreshnessConfig(BaseModel):
    inherited_from: str | None = None
    sla_seconds: int | None = None
    updates_on: Literal["any", "all"] = "any"
    # Rebuild inside the window when a direct parent's code changed.
    bypass_on_upstream_code_change: bool = False


class Node(BaseModel):
    last_updated: datetime | None = None
    node_type: NodeType


class SourceNode(Node):
    node_type: NodeType = NodeType.SOURCE


class MaterialisationNode(Node):
    node_type: NodeType = NodeType.MATERIALISATION

    asset_external_id: str
    checksum: str
    dbt_path: str
    file_path: str
    freshness_config: FreshnessConfig
    freshness: Freshness
    reason: str
    code_changed: bool = False
    sources: dict[str, datetime]
    # Fully-rendered warehouse relation; unset for ephemeral models.
    relation_name: str | None = None
    # Properties file as `<package>://<path from the package root>`; unset when none.
    patch_path: str | None = None


class Edge(BaseModel):
    from_: str
    to_: str


class ParsedDag(BaseModel):
    nodes: dict[str, Node]
    edges: list[Edge]
