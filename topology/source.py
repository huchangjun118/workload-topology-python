"""In-memory data source simulation.

A DataSource owns a change log and pushes batches into the store
through an in-process pipe. Disconnect/reconnect models flaky sources:
while disconnected, produced changes are buffered and replayed (with
their original sequence numbers) on reconnect, so no update is lost
and the store never regresses.
"""

from __future__ import annotations

import threading
from typing import List, Tuple

from .models import Change
from .store import TopologyStore


class DataSource:
    def __init__(self, source_id: str, store: TopologyStore) -> None:
        self.source_id = source_id
        self._store = store
        self._seq = 0
        self._connected = False
        self._buffer: List[Tuple[int, List[Change]]] = []
        self._lock = threading.Lock()

    def connect(self) -> None:
        with self._lock:
            if self._connected:
                return
            self._connected = True
            self._store.connect(self.source_id)
            pending = self._buffer
            self._buffer = []
        # replay buffered batches in original order (catch-up after outage)
        for seq, changes in pending:
            self._store.apply(self.source_id, seq, changes)

    def disconnect(self) -> None:
        with self._lock:
            if not self._connected:
                return
            self._connected = False
        self._store.disconnect(self.source_id)

    @property
    def connected(self) -> bool:
        return self._connected

    def emit(self, changes: List[Change]) -> None:
        """Push a batch; buffered while disconnected, replayed on reconnect."""
        with self._lock:
            self._seq += 1
            seq = self._seq
            connected = self._connected
            if not connected:
                self._buffer.append((seq, list(changes)))
                return
        self._store.apply(self.source_id, seq, list(changes))
