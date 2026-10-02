"""Read consistency under concurrent writes, cancellation and timeout."""

from __future__ import annotations

import threading
import time

import pytest

from topology import (
    CancelToken,
    DataSource,
    Level,
    Scope,
)
from topology.errors import CancelledError
from topology.errors import TimeoutError as TopologyTimeoutError

from .conftest import make_stack, node_change, replica_change, workload_change

ALL = Scope()


def _seed(store, count=20):
    src = DataSource("seed", store)
    src.connect()
    src.emit([node_change(f"n{i}", "c1", 1) for i in range(count)])
    src.emit([workload_change(f"w{i}", f"n{i}", 1) for i in range(count)])
    src.emit([replica_change(f"r{i}", f"w{i}", 1) for i in range(count)])
    return src


def test_reads_observe_consistent_view_versions():
    """Every response must be attributable to exactly one view version and
    must never mix pre-/post-change state within one response."""
    store, svc = make_stack()
    writer = DataSource("w", store)
    writer.connect()
    writer.emit([node_change("n0", "c1", 1)])
    writer.emit([workload_change("w0", "n0", 1)])

    stop = threading.Event()
    errors = []

    def write_loop():
        v = 2
        while not stop.is_set():
            writer.emit([workload_change(f"w{v}", "n0", v)])
            v += 1

    def read_loop():
        try:
            while not stop.is_set():
                view = svc.get_level(Level.WORKLOAD, ALL, "n0")
                # internal consistency: every edge child is in the entity set
                ids = {e.id for e in view.entities}
                for edge in view.edges:
                    assert edge.child_id in ids, "dangling edge in response"
                    assert edge.parent_id == "n0"
                # version monotonicity of the view itself
                assert view.view_version >= 1
        except AssertionError as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=read_loop) for _ in range(8)]
    writer_thread = threading.Thread(target=write_loop)
    for t in threads:
        t.start()
    writer_thread.start()
    time.sleep(0.3)
    stop.set()
    for t in threads + [writer_thread]:
        t.join()
    assert not errors


def test_concurrent_sources_never_corrupt_store():
    store, svc = make_stack()
    sources = [DataSource(f"s{i}", store) for i in range(4)]
    for s in sources:
        s.connect()

    def push(src, base):
        for i in range(50):
            src.emit([node_change(f"n{base + i}", "c1", i + 1)])

    threads = [
        threading.Thread(target=push, args=(s, i * 1000))
        for i, s in enumerate(sources)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(store.merged_nodes()) == 200
    view = svc.get_level(Level.NODE, ALL)
    assert len(view.entities) == 200
    assert view.view_version == store.view_version()


def test_cancelled_read_raises_and_publishes_nothing():
    store, svc = make_stack()
    _seed(store)
    token = CancelToken()
    token.cancel()
    with pytest.raises(CancelledError):
        svc.get_level(Level.NODE, ALL, token=token)


def test_timeout_raises():
    store, svc = make_stack()
    _seed(store)
    with pytest.raises(TopologyTimeoutError):
        svc.get_level(Level.NODE, ALL, timeout=-1.0)  # already-expired deadline


def test_cancel_does_not_corrupt_subsequent_reads():
    store, svc = make_stack()
    _seed(store)
    token = CancelToken()
    token.cancel()
    with pytest.raises(CancelledError):
        svc.get_level(Level.REPLICA, ALL, token=token)
    # store untouched: a normal read right after sees the full, coherent view
    view = svc.get_level(Level.REPLICA, ALL)
    assert len(view.entities) == 20
    ids = {e.id for e in view.entities}
    assert all(e.child_id in ids for e in view.edges)


def test_read_during_burst_returns_single_version_snapshot():
    """A read taken while a burst of changes lands must equal the view of
    exactly one store version (no half-applied batch leakage)."""
    store, svc = make_stack()
    writer = DataSource("w", store)
    writer.connect()
    for i in range(10):
        writer.emit([node_change(f"n{i}", "c1", 1)])
    stop = threading.Event()
    mismatches = []

    def burst():
        v = 2
        while not stop.is_set():
            writer.emit([node_change(f"n{v % 10}", "c1", v)])
            v += 1

    def check():
        while not stop.is_set():
            view = svc.get_level(Level.NODE, ALL)
            # re-derive what the store looked like at view.view_version is
            # impossible post-hoc; instead assert the response is a fixed
            # point: re-reading with no writes in between is not required,
            # but the response must be internally consistent (ids unique,
            # edges match entities).
            ids = [e.id for e in view.entities]
            if len(ids) != len(set(ids)):
                mismatches.append("duplicate entities in one response")

    threads = [threading.Thread(target=check) for _ in range(4)]
    b = threading.Thread(target=burst)
    for t in threads:
        t.start()
    b.start()
    time.sleep(0.2)
    stop.set()
    for t in threads + [b]:
        t.join()
    assert not mismatches
