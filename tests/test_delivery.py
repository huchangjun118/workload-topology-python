"""Tests for disconnect buffering, re-delivery, out-of-order and duplicates."""

from __future__ import annotations

import asyncio

from topology.aggregator import Aggregator
from topology.service import Scope, TopologyService

from .conftest import push_all, report


async def test_disconnect_buffers_and_redelivers(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    svc = TopologyService(agg)

    await source.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await asyncio.sleep(0.15)
    assert [e.key.id for e in svc.list_clusters(Scope.all()).entities] == ["c1"]

    # Disconnect: reports are buffered, not visible.
    await source.disconnect()
    await source.push_report(report("node", "n1", "s1", 2, cluster_id="c1"))
    await source.push_report(report("node", "n2", "s1", 3, cluster_id="c1"))
    await asyncio.sleep(0.15)
    assert svc.list_nodes(Scope.all(), "c1").entities == ()

    # Reconnect: buffered events re-delivered in order.
    await source.reconnect()
    await asyncio.sleep(0.2)
    nodes = svc.list_nodes(Scope.all(), "c1")
    assert [e.key.id for e in nodes.entities] == ["n1", "n2"]
    await agg.stop()


async def test_redelivery_does_not_roll_back(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await source.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await asyncio.sleep(0.15)
    v1 = agg.version

    await source.disconnect()
    await source.push_report(report("node", "n1", "s1", 2, cluster_id="c1"))
    await source.reconnect()
    await asyncio.sleep(0.2)
    assert agg.version > v1  # never rolls back
    await agg.stop()


async def test_out_of_order_delivery_converges(make_source):
    """Shuffled delivery still converges to the correct merged state."""
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()

    events = [
        report("cluster", "c1", "s1", 1, name="c1"),
        report("node", "n1", "s1", 5, cluster_id="c1", name="n5"),
        report("node", "n1", "s1", 2, cluster_id="c1", name="n2"),
        report("node", "n2", "s1", 3, cluster_id="c1", name="n3"),
        report("workload", "w1", "s1", 4, node_id="n1", name="w4"),
    ]
    # Deliver in a shuffled order.
    for ev in [events[3], events[1], events[0], events[4], events[2]]:
        await source.push_report(ev)
    await asyncio.sleep(0.2)

    svc = TopologyService(agg)
    nodes = svc.list_nodes(Scope.all(), "c1")
    assert [e.key.id for e in nodes.entities] == ["n1", "n2"]
    # n1's winning payload is from seq 5 (highest order key).
    n1 = next(e for e in nodes.entities if e.key.id == "n1")
    assert n1.payload["name"] == "n5"
    workloads = svc.list_workloads(Scope.all(), "n1")
    assert [e.key.id for e in workloads.entities] == ["w1"]
    await agg.stop()


async def test_duplicate_redelivery_folded(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    r = report("node", "n1", "s1", 1, cluster_id="c1")
    await source.push_report(r)
    await asyncio.sleep(0.15)
    v1 = agg.version

    # Re-deliver the exact same event (at-least-once semantics).
    await source.push_report(r)
    await asyncio.sleep(0.15)
    assert agg.version == v1  # folded, no new version
    await agg.stop()


async def test_disconnect_reconnect_preserves_other_sources(make_source):
    """One source's disconnect must not affect another source's data."""
    s1 = make_source("s1")
    s2 = make_source("s2")
    agg = Aggregator([s1, s2])
    await agg.start()
    await s1.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await s2.push_report(report("cluster", "c2", "s2", 1, name="c2"))
    await asyncio.sleep(0.2)

    await s1.disconnect()
    await asyncio.sleep(0.15)
    svc = TopologyService(agg)
    clusters = svc.list_clusters(Scope.all())
    assert [e.key.id for e in clusters.entities] == ["c1", "c2"]
    # c2 still readable while s1 is disconnected.
    assert any(e.key.id == "c2" for e in clusters.entities)
    await agg.stop()
