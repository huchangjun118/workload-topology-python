"""Read-side drill-down service over the topology store."""

from __future__ import annotations

import threading
import time
from typing import Optional

from .errors import CancelledError, ForbiddenError, NotFoundError
from .errors import TimeoutError as TopologyTimeoutError
from .models import (
    EdgeView,
    EntityView,
    Freshness,
    Level,
    LevelView,
    Scope,
)
from .store import TopologyStore

_POLL_INTERVAL = 0.005


class CancelToken:
    """Cooperative cancellation token for read requests."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()


class TopologyService:
    """Serves scoped, levelled, consistent read views.

    Every response is computed from a single atomic snapshot of the
    store, so it is always attributable to exactly one view version;
    changes arriving while a read is in progress never leak into an
    in-flight response. Cancellation/timeout is checked before and
    after the snapshot is taken, so a cancelled read never publishes
    a half-built result.
    """

    def __init__(self, store: TopologyStore) -> None:
        self._store = store

    # ---- public API ------------------------------------------------

    def get_level(
        self,
        level: Level,
        scope: Scope,
        parent_id: Optional[str] = None,
        *,
        timeout: Optional[float] = None,
        token: Optional[CancelToken] = None,
    ) -> LevelView:
        """Return entities and edges directly related to ``level``.

        ``parent_id`` drills down: CLUSTER listing takes no parent,
        NODE takes a cluster id, WORKLOAD a node id, REPLICA a
        workload id. A parent that does not exist raises NotFound;
        a parent outside ``scope`` raises Forbidden.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        self._check_cancel(token, deadline)

        # one atomic snapshot -> one consistent view version
        snapshot = self._snapshot()

        self._check_cancel(token, deadline)

        builder = {
            Level.CLUSTER: self._cluster_level,
            Level.NODE: self._node_level,
            Level.WORKLOAD: self._workload_level,
            Level.REPLICA: self._replica_level,
        }[level]
        entities, edges = builder(snapshot, scope, parent_id)

        self._check_cancel(token, deadline)

        return LevelView(
            level=level,
            view_version=snapshot["view_version"],
            entities=tuple(entities),
            edges=tuple(edges),
            sources=snapshot["sources"],
        )

    # ---- snapshot --------------------------------------------------

    def _snapshot(self) -> dict:
        """Take an atomic, internally consistent snapshot of the store."""
        lock = self._store._lock  # single lock guards all store state
        with lock:
            return {
                "view_version": self._store._view_version,
                "sources": self._store.source_statuses(),
                "nodes": self._store.merged_nodes(),
                "workloads": self._store.merged_workloads(),
                "replicas": self._store.merged_replicas(),
            }

    # ---- cancellation / timeout ------------------------------------

    @staticmethod
    def _check_cancel(token: Optional[CancelToken], deadline: Optional[float]) -> None:
        if token is not None and token.cancelled:
            raise CancelledError("read request was cancelled")
        if deadline is not None and time.monotonic() > deadline:
            raise TopologyTimeoutError("read request timed out")

    # ---- entity helpers --------------------------------------------

    @staticmethod
    def _fresh(stale: bool) -> Freshness:
        return Freshness.STALE if stale else Freshness.LIVE

    # ---- level builders --------------------------------------------
    # Each builder returns (entities, edges) containing only entities of
    # its own level plus containment edges to the immediate parent layer.

    def _cluster_level(self, snap, scope, parent_id):
        if parent_id is not None:
            raise NotFoundError(f"cluster {parent_id!r} not found")
        clusters = {}
        for node_id, (node, sources, stale, version) in snap["nodes"].items():
            if not scope.allows_cluster(node.cluster_id):
                continue
            cur = clusters.get(node.cluster_id)
            entry = {
                "stale": stale if cur is None else (cur["stale"] and stale),
                "version": version if cur is None else max(cur["version"], version),
                "sources": set() if cur is None else cur["sources"],
            }
            entry["sources"].update(sources)
            clusters[node.cluster_id] = entry
        entities = [
            EntityView(
                kind=Level.CLUSTER,
                id=cid,
                parent_id=None,
                health=_worst_health(snap, cluster_id=cid),
                freshness=self._fresh(meta["stale"]),
                version=meta["version"],
                sources=tuple(sorted(meta["sources"])),
            )
            for cid, meta in sorted(clusters.items())
        ]
        return entities, []

    def _node_level(self, snap, scope, parent_id):
        entities, edges = [], []
        for node_id, (node, sources, stale, version) in sorted(snap["nodes"].items()):
            if parent_id is not None and node.cluster_id != parent_id:
                continue
            if not (scope.allows_cluster(node.cluster_id) and scope.allows_node(node_id)):
                continue
            entities.append(
                EntityView(
                    kind=Level.NODE,
                    id=node_id,
                    parent_id=node.cluster_id,
                    health=node.health,
                    freshness=self._fresh(stale),
                    version=version,
                    sources=sources,
                    labels=node.labels,
                )
            )
            edges.append(
                EdgeView(node.cluster_id, node_id, self._fresh(stale))
            )
        if parent_id is not None:
            self._require_cluster(snap, scope, parent_id)
        return entities, edges

    def _workload_level(self, snap, scope, parent_id):
        entities, edges = [], []
        nodes = snap["nodes"]
        for wl_id, (wl, sources, stale, version) in sorted(snap["workloads"].items()):
            if parent_id is not None and wl.node_id != parent_id:
                continue
            node_rec = nodes.get(wl.node_id)
            if node_rec is None:
                continue  # parent node not visible: no dangling entities
            node = node_rec[0]
            if not (
                scope.allows_cluster(node.cluster_id)
                and scope.allows_node(wl.node_id)
                and scope.allows_workload(wl_id)
            ):
                continue
            edge_stale = stale or node_rec[2]
            entities.append(
                EntityView(
                    kind=Level.WORKLOAD,
                    id=wl_id,
                    parent_id=wl.node_id,
                    health=wl.health,
                    freshness=self._fresh(stale),
                    version=version,
                    sources=sources,
                    labels=wl.labels,
                )
            )
            edges.append(EdgeView(wl.node_id, wl_id, self._fresh(edge_stale)))
        if parent_id is not None:
            self._require_node(snap, scope, parent_id)
        return entities, edges

    def _replica_level(self, snap, scope, parent_id):
        entities, edges = [], []
        nodes = snap["nodes"]
        workloads = snap["workloads"]
        for rep_id, (rep, sources, stale, version) in sorted(snap["replicas"].items()):
            if parent_id is not None and rep.workload_id != parent_id:
                continue
            wl_rec = workloads.get(rep.workload_id)
            if wl_rec is None:
                continue  # parent workload not visible: no orphans
            wl = wl_rec[0]
            node_rec = nodes.get(wl.node_id)
            if node_rec is None:
                continue
            node = node_rec[0]
            if not (
                scope.allows_cluster(node.cluster_id)
                and scope.allows_node(wl.node_id)
                and scope.allows_workload(rep.workload_id)
            ):
                continue
            edge_stale = stale or wl_rec[2]
            entities.append(
                EntityView(
                    kind=Level.REPLICA,
                    id=rep_id,
                    parent_id=rep.workload_id,
                    health=rep.health,
                    freshness=self._fresh(stale),
                    version=version,
                    sources=sources,
                    labels=rep.labels,
                )
            )
            edges.append(EdgeView(rep.workload_id, rep_id, self._fresh(edge_stale)))
        if parent_id is not None:
            self._require_workload(snap, scope, parent_id)
        return entities, edges

    # ---- 404 vs 403 -------------------------------------------------

    def _require_cluster(self, snap, scope, cluster_id) -> None:
        known = {n.cluster_id for n, *_ in snap["nodes"].values()}
        if cluster_id not in known:
            raise NotFoundError(f"cluster {cluster_id!r} not found")
        if not scope.allows_cluster(cluster_id):
            raise ForbiddenError(f"cluster {cluster_id!r} is outside the caller's scope")

    def _require_node(self, snap, scope, node_id) -> None:
        rec = snap["nodes"].get(node_id)
        if rec is None:
            raise NotFoundError(f"node {node_id!r} not found")
        node = rec[0]
        if not (scope.allows_cluster(node.cluster_id) and scope.allows_node(node_id)):
            raise ForbiddenError(f"node {node_id!r} is outside the caller's scope")

    def _require_workload(self, snap, scope, workload_id) -> None:
        rec = snap["workloads"].get(workload_id)
        if rec is None:
            raise NotFoundError(f"workload {workload_id!r} not found")
        wl = rec[0]
        node_rec = snap["nodes"].get(wl.node_id)
        cluster_id = node_rec[0].cluster_id if node_rec else None
        if not (
            scope.allows_workload(workload_id)
            and scope.allows_node(wl.node_id)
            and (cluster_id is None or scope.allows_cluster(cluster_id))
        ):
            raise ForbiddenError(
                f"workload {workload_id!r} is outside the caller's scope"
            )


def _worst_health(snap, cluster_id: str):
    from .models import Health

    order = {Health.HEALTHY: 0, Health.DEGRADED: 1, Health.FAILED: 2}
    worst = Health.HEALTHY
    for node, *_ in snap["nodes"].values():
        if node.cluster_id == cluster_id and order[node.health] > order[worst]:
            worst = node.health
    return worst
