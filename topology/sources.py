"""In-process data source simulation.

Each data source pushes entity reports and heartbeats through an in-memory
async pipe (an ``asyncio.Queue``). No external cluster, credential or
network is involved.

Delivery semantics (mirroring a real at-least-once source):

* While *disconnected*, reports are buffered in the source's outbox and
  re-delivered in order when the source reconnects.
* Re-delivered or duplicated reports carry the same ``OrderKey`` and are
  folded by the aggregator.
* A *failed* source stops emitting entirely (no heartbeats, no reports);
  the aggregator detects the silence after ``fail_timeout`` and marks the
  source DEAD. On recovery the source flushes its outbox and heartbeats
  resume, so the DEAD state clears automatically.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import AsyncIterator

from .model import Report, SourceHealth, SourceState


@dataclass
class SourceConfig:
    source_id: str
    name: str = ""
    #: Cluster ids this source is responsible for covering.
    scope: frozenset[str] = field(default_factory=frozenset)
    #: Heartbeat interval; a source silent for longer than this is DEAD.
    heartbeat_interval: float = 0.5
    fail_timeout: float = 2.0


@dataclass(frozen=True)
class PipeEvent:
    """An event read from a source pipe."""

    kind: str  # "report" | "heartbeat"
    source_id: str
    report: Report | None = None


class DataSource:
    """A simulated data source backed by an in-process async pipe."""

    def __init__(self, config: SourceConfig) -> None:
        self.config = config
        self._queue: asyncio.Queue[PipeEvent] = asyncio.Queue()
        self._outbox: list[PipeEvent] = []
        self._connected = True
        self._failed = False
        self._closed = False
        self._hb_task: asyncio.Task[None] | None = None
        self._stop_hb = asyncio.Event()

    # -- properties --------------------------------------------------------

    @property
    def source_id(self) -> str:
        return self.config.source_id

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def failed(self) -> bool:
        return self._failed

    # -- emitting ----------------------------------------------------------

    async def push_report(self, report: Report) -> None:
        """Push a report.

        Buffered in the outbox while disconnected; dropped while failed
        (recovery re-sends the latest state anyway).
        """
        if self._closed or self._failed:
            return
        event = PipeEvent(kind="report", source_id=self.source_id, report=report)
        if self._connected:
            await self._queue.put(event)
        else:
            self._outbox.append(event)

    async def heartbeat(self) -> None:
        """Emit a single heartbeat (no-op while disconnected/failed)."""
        if self._closed or self._failed or not self._connected:
            return
        await self._queue.put(
            PipeEvent(kind="heartbeat", source_id=self.source_id)
        )

    # -- lifecycle ---------------------------------------------------------

    async def disconnect(self) -> None:
        """Stop delivering; reports stay buffered in the outbox."""
        self._connected = False

    async def reconnect(self) -> None:
        """Resume delivery; buffered events are re-delivered in order."""
        if self._failed or self._closed:
            return
        self._connected = True
        await self._flush_outbox()
        await self.heartbeat()

    async def fail(self) -> None:
        """Hard-fail: pipe stops delivering heartbeats and reports."""
        self._failed = True
        self._connected = False

    async def recover(self) -> None:
        """Recover from failure: flush outbox and resume heartbeats."""
        if self._closed:
            return
        self._failed = False
        self._connected = True
        await self._flush_outbox()
        await self.heartbeat()

    async def close(self) -> None:
        """Close the pipe permanently."""
        self._closed = True
        self._connected = False
        self._stop_hb.set()
        if self._hb_task is not None:
            self._hb_task.cancel()
            try:
                await self._hb_task
            except asyncio.CancelledError:
                pass

    # -- background heartbeat ---------------------------------------------

    def start_heartbeats(self) -> None:
        """Start emitting heartbeats in the background while connected."""
        if self._hb_task is not None:
            return
        self._stop_hb.clear()
        self._hb_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        while not self._stop_hb.is_set():
            await self.heartbeat()
            try:
                await asyncio.wait_for(
                    self._stop_hb.wait(), timeout=self.config.heartbeat_interval
                )
            except asyncio.TimeoutError:
                pass

    # -- consuming ---------------------------------------------------------

    async def events(self) -> AsyncIterator[PipeEvent]:
        """Async iterator of pipe events (re-delivered after reconnect)."""
        while not self._closed:
            try:
                event = await self._queue.get()
            except asyncio.CancelledError:
                return
            yield event

    # -- internals ---------------------------------------------------------

    async def _flush_outbox(self) -> None:
        for event in self._outbox:
            await self._queue.put(event)
        self._outbox.clear()
