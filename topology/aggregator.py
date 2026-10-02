"""Topology aggregation.

The aggregator consumes events from all source pipes, keeps per-source
latest reports and merges them into immutable *snapshots*. Reads always
reference a snapshot, so in-flight reads see a consistent view even while
changes keep arriving.

Merge rules (deterministic, arrival-order independent):

* For every entity key, the report with the greatest ``OrderKey`` wins.
* A winning tombstone (``deleted=True``) removes the entity; an older
  report can never resurrect it.
* An entity is ``STALE`` when at least one source reports it but none of
  its contributing sources is alive; otherwise it is ``ACTIVE``.

Snapshots are built synchronously in one non-interruptible step and
published by swapping a single reference, so cancellation can never leave
the aggregation in an intermediate state.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator

from .model import (
    Edge,
    Entity,
    EntityKey,
    EntityStatus,
    Report,
    SourceHealth,
    SourceState,
)
from .sources import DataSource, PipeEvent


class Snapshot:
    """An immutable, consistent view of the topology at ``version``."""

    __slots__ = ("version", "entities", "edges", "source_health", "created_at_ns")

    def __init__(
        self,
        version: int,
        entities: dict[EntityKey, Entity],
        edges: dict[EntityKey, tuple[Edge, ...]],
        source_health: dict[str, SourceHealth],
        created_at_ns: int,
    ) -> None:
        self.version = version
        self.entities = entities
        self.edges = edges
        self.source_health = source_health
        self.created_at_ns = created_at_ns


class Aggregator:
    """Merges source events into published snapshots."""

    def __init__(self, sources: list[DataSource], max_history: int = 100) -> None:
        self._sources: dict[str, DataSource] = {s.source_id: s for s in sources}
        # source_id -> entity_key -> latest report
        self._per_source: dict[str, dict[EntityKey, Report]] = {
            sid: {} for sid in self._sources
        }
        self._snapshot: Snapshot = self._empty_snapshot(0)
        self._version = 0
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._pump_tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()
        self._shared_queue: asyncio.Queue[PipeEvent] = asyncio.Queue()
        now = time.monotonic_ns()
        self._last_seen_ns: dict[str, int] = {sid: now for sid in self._sources}
        self._last_liveness: dict[str, SourceState] = self._compute_liveness()
        # Immutable snapshot history so clients can pin a view version.
        self._history: dict[int, Snapshot] = {0: self._snapshot}
        self._max_history = max_history

    # -- snapshots ---------------------------------------------------------

    @property
    def version(self) -> int:
        return self._version

    def current_snapshot(self) -> Snapshot:
        """Return the latest published snapshot.

        The returned object is immutable; readers may use it without
        locking while the aggregator keeps publishing new versions.
        """
        return self._snapshot

    def snapshot_at(self, version: int) -> Snapshot | None:
        """Return the snapshot published at ``version``, or None.

        Clients pin a view version so that several drill-down calls can
        all be served from the same consistent view.
        """
        return self._history.get(version)

    @staticmethod
    def _empty_snapshot(version: int) -> Snapshot:
        return Snapshot(
            version=version,
            entities={},
            edges={},
            source_health={},
            created_at_ns=time.time_ns(),
        )

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        """Start consuming source pipes."""
        if self._task is not None:
            return
        self._stop.clear()
        for source in self._sources.values():
            source.start_heartbeats()
            self._pump_tasks.append(
                asyncio.create_task(self._pump(source), name=f"pump-{source.source_id}")
            )
        self._task = asyncio.create_task(self._run(), name="aggregator")

    async def stop(self) -> None:
        """Stop consuming and close all source pipes."""
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        for task in self._pump_tasks:
            task.cancel()
        for task in self._pump_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._pump_tasks.clear()
        for source in self._sources.values():
            await source.close()

    async def _pump(self, source: DataSource) -> None:
        """Forward events from one source pipe into the shared queue."""
        async for event in source.events():
            await self._shared_queue.put(event)

    # -- event loop --------------------------------------------------------

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                event = await asyncio.wait_for(self._shared_queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                event = None
            events: list[PipeEvent] = []
            if event is not None:
                events.append(event)
                # drain everything currently buffered so a burst of changes
                # is applied as one new version (atomic batch).
                while True:
                    try:
                        events.append(self._shared_queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
            self._apply_events(events)

    # -- merge (synchronous, non-interruptible) ----------------------------

    def _apply_events(self, events: list[PipeEvent]) -> None:
        """Apply a batch of events and publish a new snapshot.

        Synchronous on purpose: there are no ``await`` points between
        reading the per-source state and swapping the snapshot reference,
        so task cancellation can never interrupt a merge half-way.
        """
        state_changed = False
        for event in events:
            source_id = event.source_id
            if source_id not in self._per_source:
                continue
            self._last_seen_ns[source_id] = time.monotonic_ns()
            if event.kind == "report" and event.report is not None:
                report = event.report
                per = self._per_source[source_id]
                existing = per.get(report.key)
                # Fold duplicates: keep the greatest OrderKey only.
                if existing is None or report.order > existing.order:
                    per[report.key] = report
                    state_changed = True
            # heartbeats only affect liveness; the check below rebuilds
            # only when liveness actually changes.

        liveness = self._compute_liveness()
        if not state_changed and liveness == self._last_liveness:
            return

        self._version += 1
        self._snapshot = self._build_snapshot(self._version, liveness)
        self._last_liveness = liveness
        self._history[self._version] = self._snapshot
        if len(self._history) > self._max_history:
            oldest = min(self._history)
            del self._history[oldest]

    def _compute_liveness(self) -> dict[str, SourceState]:
        now = time.monotonic_ns()
        result: dict[str, SourceState] = {}
        for sid, source in self._sources.items():
            if source.failed:
                result[sid] = SourceState.DEAD
            elif now - self._last_seen_ns[sid] > source.config.fail_timeout * 1e9:
                result[sid] = SourceState.DEAD
            else:
                result[sid] = SourceState.ALIVE
        return result

    def _build_snapshot(
        self, version: int, liveness: dict[str, SourceState]
    ) -> Snapshot:
        # 1. merge per-source reports: greatest OrderKey wins.
        merged: dict[EntityKey, Report] = {}
        contributors: dict[EntityKey, set[str]] = {}
        for sid, per in self._per_source.items():
            for key, report in per.items():
                current = merged.get(key)
                if current is None or report.order > current.order:
                    merged[key] = report
                contributors.setdefault(key, set()).add(sid)

        # 2. materialize entities (tombstones remove; status from liveness).
        entities: dict[EntityKey, Entity] = {}
        for key, report in merged.items():
            if report.deleted:
                continue
            contrib = frozenset(contributors[key])
            status = (
                EntityStatus.ACTIVE
                if any(liveness.get(s) == SourceState.ALIVE for s in contrib)
                else EntityStatus.STALE
            )
            entities[key] = Entity(
                key=key,
                status=status,
                payload=dict(report.payload),
                contributors=contrib,
            )

        # 3. derive edges from foreign keys, dropping dangling references.
        edges = self._build_edges(entities)

        health = {
            sid: SourceHealth(source_id=sid, state=state)
            for sid, state in liveness.items()
        }
        return Snapshot(
            version=version,
            entities=entities,
            edges=edges,
            source_health=health,
            created_at_ns=time.time_ns(),
        )

    def _build_edges(
        self, entities: dict[EntityKey, Entity]
    ) -> dict[EntityKey, tuple[Edge, ...]]:
        edges: dict[EntityKey, list[Edge]] = {}
        for key, entity in entities.items():
            parent = self._parent_key(key, entity.payload)
            if parent is None or parent not in entities:
                continue  # dangling reference: no edge, no orphan in response
            edges.setdefault(parent, []).append(Edge(parent=parent, child=key))
        return {p: tuple(sorted(es, key=lambda e: e.child.id)) for p, es in edges.items()}

    @staticmethod
    def _parent_key(key: EntityKey, payload: dict) -> EntityKey | None:
        from .model import EntityType, cluster_key, node_key, replica_key, workload_key

        if key.type is EntityType.NODE:
            cid = payload.get("cluster_id")
            return cluster_key(cid) if cid is not None else None
        if key.type is EntityType.WORKLOAD:
            nid = payload.get("node_id")
            return node_key(nid) if nid is not None else None
        if key.type is EntityType.REPLICA:
            wid = payload.get("workload_id")
            return workload_key(wid) if wid is not None else None
        return None
