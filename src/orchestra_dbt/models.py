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


class RelationType(str, Enum):
    """A relation's kind in the warehouse; mirrors dbt's own RelationType."""

    TABLE = "table"
    VIEW = "view"
    CTE = "cte"
    MATERIALIZED_VIEW = "materialized_view"
    EPHEMERAL = "ephemeral"
    EXTERNAL = "external"
    POINTER_TABLE = "pointer_table"
    FUNCTION = "function"

    @classmethod
    def parse(cls, value: object) -> "RelationType | None":
        """Leniently coerce a stored or adapter-supplied value.

        Unknown kinds deliberately become None rather than raising: the
        relation type only ever refines a freshness decision, so a value we
        don't recognise should fall back to the default behaviour instead of
        failing the run.
        """
        # Adapters hand back their own enum member, so read `.value` when
        # present -- str() on an Enum can render as "RelationType.VIEW".
        raw = getattr(value, "value", value)
        if not raw:
            return None
        try:
            return cls(str(raw).strip().lower())
        except ValueError:
            return None


class StateItem(BaseModel):
    last_updated: datetime
    checksum: str
    sources: dict[str, datetime]


class StateApiModel(BaseModel):
    state: dict[str, StateItem]
    # Cache of relation name -> RelationType value, e.g.
    # "DB.schema_raw.orders" -> "view". Keyed by relation rather than source
    # id because a source resolves to a different relation per target, and
    # one state file can serve several. Populated once and reused across
    # runs, since a relation's kind essentially never changes. Stored as
    # plain strings so an unfamiliar value read back from persisted state can
    # never fail validation -- see RelationType.parse.
    source_relation_types: dict[str, str] = {}


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
    # The relation's kind in the warehouse, when known. Only looked up for
    # sources whose freshness comes from relation metadata, since that is
    # the only case where the kind changes how `last_updated` is read.
    relation_type: RelationType | None = None


class MaterialisationNode(Node):
    node_type: NodeType = NodeType.MATERIALISATION

    asset_external_id: str
    checksum: str
    dbt_path: str
    file_path: str
    freshness_config: FreshnessConfig
    freshness: Freshness
    reason: str
    sources: dict[str, datetime]
    # Fully-rendered warehouse relation; unset for ephemeral models.
    relation_name: str | None = None


class Edge(BaseModel):
    from_: str
    to_: str


class ParsedDag(BaseModel):
    nodes: dict[str, Node]
    edges: list[Edge]
