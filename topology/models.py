"""Core domain models for the workload topology service.

All entities are immutable value objects. Versions are per-entity,
monotonically increasing integers supplied by the reporting data source.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Tuple


class Health(enum.Enum):
    """Health of an entity as reported by a data source."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"


class Freshness(enum.Enum):
    """Freshness marker attached by the aggregator (never by sources)."""

    LIVE = "live"
    STALE = "stale"


class Level(enum.Enum):
    """Drill-down levels of the topology."""

    CLUSTER = "cluster"
    NODE = "node"
    WORKLOAD = "workload"
    REPLICA = "replica"


@dataclass(frozen=True)
class Node:
    id: str
    cluster_id: str
    labels: Tuple[Tuple[str, str], ...] = ()
    health: Health = Health.HEALTHY
    version: int = 0


@dataclass(frozen=True)
class Workload:
    id: str
    node_id: str
    kind: str = "deployment"
    labels: Tuple[Tuple[str, str], ...] = ()
    health: Health = Health.HEALTHY
    version: int = 0


@dataclass(frozen=True)
class Replica:
    id: str
    workload_id: str
    labels: Tuple[Tuple[str, str], ...] = ()
    health: Health = Health.HEALTHY
    version: int = 0


@dataclass(frozen=True)
class Scope:
    """Caller visibility scope.

    ``None`` for a set means "unrestricted" along that dimension.
    An entity is visible only if every dimension matches.
    """

    clusters: Optional[FrozenSet[str]] = None
    nodes: Optional[FrozenSet[str]] = None
    workloads: Optional[FrozenSet[str]] = None

    def allows_cluster(self, cluster_id: str) -> bool:
        return self.clusters is None or cluster_id in self.clusters

    def allows_node(self, node_id: str) -> bool:
        return self.nodes is None or node_id in self.nodes

    def allows_workload(self, workload_id: str) -> bool:
        return self.workloads is None or workload_id in self.workloads


@dataclass(frozen=True)
class Change:
    """A single upsert/delete pushed by a data source."""

    kind: Level  # NODE / WORKLOAD / REPLICA (CLUSTER level is implicit)
    entity_id: str
    version: int
    delete: bool = False
    node: Optional[Node] = None
    workload: Optional[Workload] = None
    replica: Optional[Replica] = None


@dataclass(frozen=True)
class SourceStatus:
    source_id: str
    connected: bool
    last_seq: int


@dataclass(frozen=True)
class EntityView:
    """An entity as returned to callers, with aggregation metadata."""

    kind: Level
    id: str
    parent_id: Optional[str]
    health: Health
    freshness: Freshness
    version: int
    sources: Tuple[str, ...] = ()
    labels: Tuple[Tuple[str, str], ...] = ()


@dataclass(frozen=True)
class EdgeView:
    """A containment edge parent -> child."""

    parent_id: str
    child_id: str
    freshness: Freshness


@dataclass(frozen=True)
class LevelView:
    """Response of a drill-down query, attributable to one view version."""

    level: Level
    view_version: int
    entities: Tuple[EntityView, ...] = ()
    edges: Tuple[EdgeView, ...] = ()
    sources: Tuple[SourceStatus, ...] = ()
