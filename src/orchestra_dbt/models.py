from datetime import datetime
from enum import Enum

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


class Node(BaseModel):
    last_updated: datetime | None = None
    node_type: NodeType


class SourceNode(Node):
    node_type: NodeType = NodeType.SOURCE


class MaterialisationNode(Node):
    node_type: NodeType = NodeType.MATERIALISATION

    checksum: str
    freshness: Freshness
    node_path: str
    reason: str
    sources: dict[str, datetime]
    sql_path: str

    freshness_config: dict | None = None


class Edge(BaseModel):
    from_: str
    to_: str


class ParsedDag(BaseModel):
    nodes: dict[str, Node]
    edges: list[Edge]
