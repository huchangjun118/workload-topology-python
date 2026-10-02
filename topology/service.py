"""Topology query service with hierarchical drill-down and scope filtering.

Every read is served from a single immutable :class:`Snapshot`, so a
response is internally consistent and attributable to one view version.

Drill-down layers:

    clusters -> nodes -> workloads -> replicas

Each layer returns only the entities and the direct parent->child edges
belonging to that layer. Entities outside the caller's visibility scope
(and their incident edges) are stripped at every layer.

Authorization is distinguished from existence:

* entity exists but is outside the caller's scope -> ``ForbiddenError``
* entity does not exist in the merged graph     -> ``NotFoundError``
"""

from __future__ import annotations

from dataclasses import dataclass

from .aggregator import Aggregator, Snapshot
from .model import (
    Edge,
    Entity,
    EntityKey,
    EntityType,
    EntityStatus,
)


class TopologyError(Exception):
    """Base class for topology service errors."""


class NotFoundError(TopologyError):
    """Raised when the requested entity does not exist."""


class ForbiddenError(TopologyError):
    """Raised when the entity exists but is outside the caller's scope."""


class ViewNotFoundError(TopologyError):
    """Raised when a pinned view version is no longer available."""


@dataclass(frozen=True)
class Scope:
    """Caller visibility scope.

    ``visible_clusters`` is the set of cluster ids the caller may see.
    ``all_clusters=True`` grants unrestricted access.
    """

    visible_clusters: frozenset[str] = frozenset()
    all_clusters: bool = False

    def can_see_cluster(self, cluster_id: str) -> bool:
        return self.all_clusters or cluster_id in self.visible_clusters

    @classmethod
    def all(cls) -> "Scope":
        return cls(all_clusters=True)


@dataclass(frozen=True)
class LayerView:
    """One layer of the drill-down response."""

    layer: EntityType
    view_version: int
    entities: tuple[Entity, ...]
    edges: tuple[Edge, ...]


class TopologyService:
    def __init__(self, aggregator: Aggregator) -> None:
        self._aggregator = aggregator

    def _snapshot(self, view: int | None = None) -> Snapshot:
        if view is None:
            return self._aggregator.current_snapshot()
        snap = self._aggregator.snapshot_at(view)
        if snap is None:
            raise ViewNotFoundError(f"view version {view} is no longer available")
        return snap

    # -- layer 0: clusters --------------------------------------------------

    def list_clusters(self, scope: Scope, view: int | None = None) -> LayerView:
        snap = self._snapshot(view)
        entities = tuple(
            sorted(
                (
                    e
                    for e in snap.entities.values()
                    if e.key.type is EntityType.CLUSTER
                    and scope.can_see_cluster(e.key.id)
                ),
                key=lambda e: e.key.id,
            )
        )
        return LayerView(
            layer=EntityType.CLUSTER,
            view_version=snap.version,
            entities=entities,
            edges=(),
        )

    # -- layer 1: nodes of a cluster ---------------------------------------

    def list_nodes(
        self, scope: Scope, cluster_id: str, view: int | None = None
    ) -> LayerView:
        snap = self._snapshot(view)
        cluster_key = EntityKey(EntityType.CLUSTER, cluster_id)
        if cluster_key not in snap.entities:
            raise NotFoundError(f"cluster {cluster_id!r} does not exist")
        if not scope.can_see_cluster(cluster_id):
            raise ForbiddenError(f"cluster {cluster_id!r} is outside the caller's scope")
        return self._children_view(
            snap,
            layer=EntityType.NODE,
            parent_key=cluster_key,
            scope=scope,
        )

    # -- layer 2: workloads of a node --------------------------------------

    def list_workloads(
        self, scope: Scope, node_id: str, view: int | None = None
    ) -> LayerView:
        snap = self._snapshot(view)
        node_key = EntityKey(EntityType.NODE, node_id)
        node = snap.entities.get(node_key)
        if node is None:
            raise NotFoundError(f"node {node_id!r} does not exist")
        self._check_scope_for_entity(snap, node, scope)
        return self._children_view(
            snap,
            layer=EntityType.WORKLOAD,
            parent_key=node_key,
            scope=scope,
        )

    # -- layer 3: replicas of a workload -----------------------------------

    def list_replicas(
        self, scope: Scope, workload_id: str, view: int | None = None
    ) -> LayerView:
        snap = self._snapshot(view)
        workload_key = EntityKey(EntityType.WORKLOAD, workload_id)
        workload = snap.entities.get(workload_key)
        if workload is None:
            raise NotFoundError(f"workload {workload_id!r} does not exist")
        self._check_scope_for_entity(snap, workload, scope)
        return self._children_view(
            snap,
            layer=EntityType.REPLICA,
            parent_key=workload_key,
            scope=scope,
        )

    # -- internals ----------------------------------------------------------

    def _children_view(
        self,
        snap: Snapshot,
        layer: EntityType,
        parent_key: EntityKey,
        scope: Scope,
    ) -> LayerView:
        edges = snap.edges.get(parent_key, ())
        entities: list[Entity] = []
        kept_edges: list[Edge] = []
        for edge in edges:
            child = snap.entities.get(edge.child)
            if child is None or child.key.type is not layer:
                continue
            if not self._entity_in_scope(snap, child, scope):
                continue
            entities.append(child)
            kept_edges.append(edge)
        entities.sort(key=lambda e: e.key.id)
        kept_edges.sort(key=lambda e: e.child.id)
        return LayerView(
            layer=layer,
            view_version=snap.version,
            entities=tuple(entities),
            edges=tuple(kept_edges),
        )

    def _check_scope_for_entity(
        self, snap: Snapshot, entity: Entity, scope: Scope
    ) -> None:
        """Raise ForbiddenError if ``entity``'s cluster is out of scope."""
        if not self._entity_in_scope(snap, entity, scope):
            raise ForbiddenError(
                f"{entity.key.type.value} {entity.key.id!r} is outside the caller's scope"
            )

    def _entity_in_scope(self, snap: Snapshot, entity: Entity, scope: Scope) -> bool:
        cluster_id = self._cluster_id_of(snap, entity)
        if cluster_id is None:
            return False
        return scope.can_see_cluster(cluster_id)

    @staticmethod
    def _cluster_id_of(snap: Snapshot, entity: Entity) -> str | None:
        """Resolve the cluster id that owns ``entity`` (transitively)."""
        if entity.key.type is EntityType.CLUSTER:
            return entity.key.id
        if entity.key.type is EntityType.NODE:
            cid = entity.payload.get("cluster_id")
            return cid if isinstance(cid, str) else None
        if entity.key.type is EntityType.WORKLOAD:
            node_id = entity.payload.get("node_id")
            if not isinstance(node_id, str):
                return None
            node = snap.entities.get(EntityKey(EntityType.NODE, node_id))
            if node is None:
                return None
            cid = node.payload.get("cluster_id")
            return cid if isinstance(cid, str) else None
        if entity.key.type is EntityType.REPLICA:
            workload_id = entity.payload.get("workload_id")
            if not isinstance(workload_id, str):
                return None
            workload = snap.entities.get(EntityKey(EntityType.WORKLOAD, workload_id))
            if workload is None:
                return None
            node_id = workload.payload.get("node_id")
            if not isinstance(node_id, str):
                return None
            node = snap.entities.get(EntityKey(EntityType.NODE, node_id))
            if node is None:
                return None
            cid = node.payload.get("cluster_id")
            return cid if isinstance(cid, str) else None
        return None
