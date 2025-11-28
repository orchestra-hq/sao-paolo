from datetime import datetime
from enum import Enum
from pydantic import BaseModel


class Freshness(Enum):
    CLEAN = "CLEAN"
    DIRTY = "DIRTY"


class NodeType(Enum):
    MODEL = "MODEL"
    SOURCE = "SOURCE"


class StateItem(BaseModel):
    last_updated: datetime
    checksum: str | None


class StateApiModel(BaseModel):
    state: dict[str, StateItem]


class SourceFreshness(BaseModel):
    sources: dict[str, datetime]


class Node(BaseModel):
    freshness: Freshness
    type: NodeType

    checksum: str | None = None
    last_updated: datetime | None = None
    freshness_config: dict | None = None
    sql_path: str | None = None


class Edge(BaseModel):
    from_: str
    to_: str


class ParsedDag(BaseModel):
    nodes: dict[str, Node]
    edges: list[Edge]
