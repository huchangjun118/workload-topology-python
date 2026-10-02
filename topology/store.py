"""Versioned, multi-source topology store (aggregation core)."""

from __future__ import annotations

import threading
from typing import Dict, List, Optional, Tuple

from .models import (
    Change,
    Level,
    Node,
    Replica,
    SourceStatus,
    Workload,
)

# per-entity table: entity id -> {source id -> (version, record or None for delete)}
_NodeTable = Dict[str, Dict[str, Tuple[int, Optional[Node]]]]
_WorkloadTable = Dict[str, Dict[str, Tuple[int, Optional[Workload]]]]
_ReplicaTable = Dict[str, Dict[str, Tuple[int, Optional[Replica]]]]


class TopologyStore:
    """Aggregates change streams from multiple in-memory data sources.

    Merge rule (deterministic, order-independent):
      * per (entity, source) only the highest-version record is kept;
      * the visible record is the live record with the highest version,
        ties broken by source id (lexicographic);
      * an entity disappears only when every source deleted it.

    Staleness: an entity is stale iff every source currently reporting
    it is disconnected. It clears automatically when any contributing
    source reconnects.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._view_version = 0
        self._nodes: _NodeTable = {}
        self._workloads: _WorkloadTable = {}
        self._replicas: _ReplicaTable = {}
        self._connected: Dict[str, bool] = {}
        self._last_seq: Dict[str, int] = {}

    # ---- ingestion -------------------------------------------------

    def connect(self, source_id: str) -> None:
        with self._lock:
            self._connected[source_id] = True
            self._last_seq.setdefault(source_id, 0)
            self._view_version += 1

    def disconnect(self, source_id: str) -> None:
        with self._lock:
            self._connected[source_id] = False
            self._view_version += 1

    def apply(self, source_id: str, seq: int, changes: List[Change]) -> int:
        """Apply a batch from a source; returns the current view version.

        Batches with ``seq`` not greater than the last applied seq for
        this source are folded away (idempotent replay / duplicates).
        """
        with self._lock:
            self._connected.setdefault(source_id, True)
            last = self._last_seq.get(source_id, 0)
            if seq <= last:
                return self._view_version
            self._last_seq[source_id] = seq
            for change in changes:
                self._apply_one(source_id, change)
            self._view_version += 1
            return self._view_version

    def _apply_one(self, source_id: str, change: Change) -> None:
        if change.kind is Level.NODE:
            table: Dict[str, Dict[str, Tuple[int, object]]] = self._nodes
            record: object = change.node
        elif change.kind is Level.WORKLOAD:
            table = self._workloads
            record = change.workload
        elif change.kind is Level.REPLICA:
            table = self._replicas
            record = change.replica
        else:
            raise ValueError(f"unsupported change kind: {change.kind}")

        if change.delete:
            record = None
        elif record is None:
            raise ValueError("upsert change must carry its entity payload")

        per_source = table.setdefault(change.entity_id, {})
        current = per_source.get(source_id)
        # last-write-wins per (entity, source): fold out-of-order/duplicates
        if current is not None and change.version <= current[0]:
            return
        per_source[source_id] = (change.version, record)

    # ---- snapshots -------------------------------------------------

    def view_version(self) -> int:
        with self._lock:
            return self._view_version

    def source_statuses(self) -> Tuple[SourceStatus, ...]:
        with self._lock:
            return tuple(
                SourceStatus(sid, self._connected.get(sid, True), self._last_seq.get(sid, 0))
                for sid in sorted(self._last_seq)
            )

    def stale_sources(self) -> frozenset:
        with self._lock:
            return frozenset(
                sid for sid, connected in self._connected.items() if not connected
            )

    def merged_nodes(self) -> Dict[str, Tuple[Node, Tuple[str, ...], bool, int]]:
        with self._lock:
            return self._merge(self._nodes)

    def merged_workloads(self) -> Dict[str, Tuple[Workload, Tuple[str, ...], bool, int]]:
        with self._lock:
            return self._merge(self._workloads)

    def merged_replicas(self) -> Dict[str, Tuple[Replica, Tuple[str, ...], bool, int]]:
        with self._lock:
            return self._merge(self._replicas)

    def _merge(self, table):
        """Collapse per-source records into one visible record per entity.

        Deterministic and independent of arrival order: the winner is the
        live record with the highest version, ties broken by source id.
        """
        merged = {}
        for entity_id, per_source in table.items():
            live = {sid: rec for sid, rec in per_source.items() if rec[1] is not None}
            if not live:
                continue  # deleted by every reporting source
            winner_sid = min(live, key=lambda sid: (-live[sid][0], sid))
            version, record = live[winner_sid]
            sources = tuple(sorted(live))
            stale = all(not self._connected.get(sid, True) for sid in live)
            merged[entity_id] = (record, sources, stale, version)
        return merged
