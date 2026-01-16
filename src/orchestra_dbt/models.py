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
    minutes_sla: int | None = None
    updates_on: Literal["any", "all"] = "any"


class Node(BaseModel):
    last_updated: datetime | None = None
    node_type: NodeType


class SourceNode(Node):
    node_type: NodeType = NodeType.SOURCE


class MaterialisationNode(Node):
    node_type: NodeType = NodeType.MATERIALISATION

    checksum: str
    freshness_config: FreshnessConfig
    freshness: Freshness
    node_path: str
    reason: str
    sources: dict[str, datetime]
    sql_path: str


class Edge(BaseModel):
    from_: str
    to_: str


class ParsedDag(BaseModel):
    nodes: dict[str, Node]
    edges: list[Edge]
