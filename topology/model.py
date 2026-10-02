"""Core data types for the workload topology service.

The topology is a four-level hierarchy:

    cluster -> node -> workload -> replica

Every entity is reported by one or more in-process data sources. Reports
carry a totally ordered *order key* so that merging is deterministic and
independent of arrival order (see Aggregator for the merge rules).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from functools import total_ordering
from typing import Any


class EntityType(str, Enum):
    CLUSTER = "cluster"
    NODE = "node"
    WORKLOAD = "workload"
    REPLICA = "replica"


class EntityStatus(str, Enum):
    """Liveness status of an entity in a merged view.

    ACTIVE -- at least one contributing source is alive.
    STALE  -- all contributing sources have failed/disconnected; the entity
              is still readable but its data may be out of date.
    """

    ACTIVE = "active"
    STALE = "stale"


class SourceState(str, Enum):
    ALIVE = "alive"
    DEAD = "dead"


# Hierarchy level index: cluster=0 ... replica=3
LEVELS: tuple[EntityType, ...] = (
    EntityType.CLUSTER,
    EntityType.NODE,
    EntityType.WORKLOAD,
    EntityType.REPLICA,
)

PARENT_TYPE: dict[EntityType, EntityType | None] = {
    EntityType.CLUSTER: None,
    EntityType.NODE: EntityType.CLUSTER,
    EntityType.WORKLOAD: EntityType.NODE,
    EntityType.REPLICA: EntityType.WORKLOAD,
}


@dataclass(frozen=True)
class EntityKey:
    """Identifies an entity within the topology."""

    type: EntityType
    id: str


@total_ordering
@dataclass(frozen=True)
class OrderKey:
    """Total order used to pick the winning report for an entity.

    The tuple (timestamp_ns, source_id, seq) is totally ordered, so the
    merge result is deterministic and independent of arrival order.
    """

    timestamp_ns: int
    source_id: str
    seq: int

    def _sort_tuple(self) -> tuple[int, str, int]:
        return (self.timestamp_ns, self.source_id, self.seq)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OrderKey):
            return NotImplemented
        return self._sort_tuple() == other._sort_tuple()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, OrderKey):
            return NotImplemented
        return self._sort_tuple() < other._sort_tuple()


@dataclass(frozen=True)
class Report:
    """A single entity observation pushed by a data source."""

    key: EntityKey
    order: OrderKey
    payload: dict[str, Any] = field(default_factory=dict)
    deleted: bool = False


@dataclass(frozen=True)
class Entity:
    """An entity after merging all sources for one view version."""

    key: EntityKey
    status: EntityStatus
    payload: dict[str, Any]
    contributors: frozenset[str]


@dataclass(frozen=True)
class Edge:
    """A direct parent -> child edge inside one hierarchy level."""

    parent: EntityKey
    child: EntityKey


@dataclass(frozen=True)
class SourceHealth:
    source_id: str
    state: SourceState


# ---------------------------------------------------------------------------
# Entity key helpers
# ---------------------------------------------------------------------------


def cluster_key(cluster_id: str) -> EntityKey:
    return EntityKey(EntityType.CLUSTER, cluster_id)


def node_key(node_id: str) -> EntityKey:
    return EntityKey(EntityType.NODE, node_id)


def workload_key(workload_id: str) -> EntityKey:
    return EntityKey(EntityType.WORKLOAD, workload_id)


def replica_key(replica_id: str) -> EntityKey:
    return EntityKey(EntityType.REPLICA, replica_id)


def make_order_key(timestamp_ns: int, source_id: str, seq: int) -> OrderKey:
    return OrderKey(timestamp_ns=timestamp_ns, source_id=source_id, seq=seq)
