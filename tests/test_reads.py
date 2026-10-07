"""Tests for read consistency, cancellation and timeout.

Reads are served from immutable snapshots, so a read in flight must never
observe a half-applied change, and cancellation/timeout must never leave
the aggregator in an intermediate state.
"""

from __future__ import annotations

import asyncio

import pytest

from topology.aggregator import Aggregator
from topology.service import Scope, TopologyService

from .conftest import push_all, report, wait_for_version


def _assert_snapshot_consistent(snap) -> None:
    """Every edge endpoint must exist in the same snapshot."""
    for parent, edges in snap.edges.items():
        assert parent in snap.entities
        for edge in edges:
            assert edge.child in snap.entities


async def test_held_snapshot_is_immutable_under_changes(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await push_all(source, [report("cluster", "c1", "s1", 1, name="c1")])
    await wait_for_version(agg, 1)

    held = agg.current_snapshot()
    assert held.version == 1

    # Keep pushing changes while "holding" the snapshot.
    for i in range(20):
        await source.push_report(report("node", f"n{i}", "s1", i + 2, cluster_id="c1"))
    await wait_for_version(agg, 2)

    # The held snapshot is unchanged and internally consistent.
    assert held.version == 1
    assert [e.key.id for e in held.entities.values() if e.key.type.value == "node"] == []
    _assert_snapshot_consistent(held)

    # The aggregator has moved on to a newer version.
    assert agg.version > 1
    _assert_snapshot_consistent(agg.current_snapshot())
    await agg.stop()


async def test_concurrent_reads_while_writing(make_source):
    """Readers never see a half-applied state while a writer is active."""
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await source.push_report(report("cluster", "c1", "s1", 0, name="c1"))
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)
    stop = asyncio.Event()
    violations = []
    writes_done = 0
    min_writes = 200

    async def writer():
        nonlocal writes_done
        i = 0
        while not stop.is_set():
            await source.push_report(report("node", f"n{i % 5}", "s1", i + 1, cluster_id="c1"))
            i += 1
            writes_done += 1
            await asyncio.sleep(0)

    async def reader():
        while not stop.is_set():
            view = svc.list_nodes(Scope.all(), "c1")
            # The response is attributable to exactly one view version.
            assert view.view_version == agg.snapshot_at(view.view_version).version
            # Every returned node must be reachable by an edge in the same view.
            edge_children = {e.child.id for e in view.edges}
            for entity in view.entities:
                if entity.key.id not in edge_children:
                    violations.append(f"orphan {entity.key.id} in v{view.view_version}")
            # All edges must belong to the same layer/version.
            _assert_snapshot_consistent(agg.current_snapshot())
            await asyncio.sleep(0)

    wt = asyncio.create_task(writer())
    rt = asyncio.create_task(reader())
    # Condition-based stopping: keep the pressure on until a substantial
    # number of writes have been interleaved with reads, not for a fixed
    # wall-clock duration.
    while writes_done < min_writes and not violations:
        await asyncio.sleep(0)
    stop.set()
    await wt
    await rt
    assert writes_done >= min_writes
    assert violations == []
    await agg.stop()


async def test_cancelled_read_leaves_no_intermediate_state(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await push_all(source, [report("cluster", "c1", "s1", 1, name="c1")])
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)
    started = asyncio.Event()

    async def slow_read():
        started.set()
        await asyncio.sleep(10)  # simulate a long read
        return svc.list_clusters(Scope.all())

    task = asyncio.create_task(slow_read())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Subsequent reads are unaffected and see a consistent version.
    view = svc.list_clusters(Scope.all())
    assert view.view_version == agg.version
    assert [e.key.id for e in view.entities] == ["c1"]
    _assert_snapshot_consistent(agg.current_snapshot())
    await agg.stop()


async def test_read_timeout_returns_and_state_intact(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await push_all(source, [report("cluster", "c1", "s1", 1, name="c1")])
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)

    async def slow_read():
        await asyncio.sleep(10)
        return svc.list_clusters(Scope.all())

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(slow_read(), timeout=0.1)

    # State is intact after the timeout.
    view = svc.list_clusters(Scope.all())
    assert view.view_version == agg.version
    assert [e.key.id for e in view.entities] == ["c1"]
    await agg.stop()


async def test_every_response_has_view_version(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await push_all(source, [
        report("cluster", "c1", "s1", 1, name="c1"),
        report("node", "n1", "s1", 2, cluster_id="c1"),
        report("workload", "w1", "s1", 3, node_id="n1"),
        report("replica", "r1", "s1", 4, workload_id="w1"),
    ])
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)
    for view in (
        svc.list_clusters(Scope.all()),
        svc.list_nodes(Scope.all(), "c1"),
        svc.list_workloads(Scope.all(), "n1"),
        svc.list_replicas(Scope.all(), "w1"),
    ):
        assert isinstance(view.view_version, int)
        assert view.view_version > 0
    await agg.stop()


async def test_pinned_view_survives_concurrent_writes(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await push_all(source, [report("cluster", "c1", "s1", 1, name="c1")])
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)

    v1 = agg.version
    pinned = svc.list_nodes(Scope.all(), "c1", view=v1)
    assert pinned.view_version == v1

    # Concurrent writes must not affect the pinned view.
    for i in range(10):
        await source.push_report(report("node", f"n{i}", "s1", i + 2, cluster_id="c1"))
    await wait_for_version(agg, v1 + 1)

    pinned_again = svc.list_nodes(Scope.all(), "c1", view=v1)
    assert pinned_again.view_version == v1
    assert pinned_again.entities == pinned.entities
    await agg.stop()
