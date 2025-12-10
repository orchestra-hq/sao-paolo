from datetime import datetime
from enum import Enum

from pydantic import BaseModel

ORCHESTRA_REUSED_NODE = "ORCHESTRA_REUSED_NODE"


class Freshness(Enum):
    CLEAN = "CLEAN"
    DIRTY = "DIRTY"


class NodeType(Enum):
    MODEL = "MODEL"
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
    freshness: Freshness
    type: NodeType

    checksum: str | None = None
    freshness_config: dict | None = None
    last_updated: datetime | None = None
    sources: dict[str, datetime] = {}
    sql_path: str | None = None


class Edge(BaseModel):
    from_: str
    to_: str


class ParsedDag(BaseModel):
    nodes: dict[str, Node]
    edges: list[Edge]
