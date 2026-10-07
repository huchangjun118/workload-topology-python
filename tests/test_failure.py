"""Tests for single-source failure degradation and stale marking."""

from __future__ import annotations

import asyncio

from topology.aggregator import Aggregator
from topology.model import SourceState, cluster_key
from topology.service import Scope, TopologyService

from .conftest import report, wait_for_source_state, wait_for_version


async def test_failed_source_subtree_marked_stale(make_source):
    s1 = make_source("s1")
    s2 = make_source("s2")
    agg = Aggregator([s1, s2])
    await agg.start()
    # s1 covers c1 + n1; s2 covers n2 under c1.
    await s1.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await s1.push_report(report("node", "n1", "s1", 2, cluster_id="c1"))
    await s2.push_report(report("node", "n2", "s2", 1, cluster_id="c1"))
    await wait_for_version(agg, 1)

    await s2.fail()
    await wait_for_source_state(agg, "s2", SourceState.DEAD)
    svc = TopologyService(agg)
    nodes = svc.list_nodes(Scope.all(), "c1")
    by_id = {e.key.id: e for e in nodes.entities}
    assert by_id["n1"].status.value == "active"   # s1 still alive
    assert by_id["n2"].status.value == "stale"    # only s2 reported it
    await agg.stop()


async def test_other_sources_data_remains_readable(make_source):
    s1 = make_source("s1")
    s2 = make_source("s2")
    agg = Aggregator([s1, s2])
    await agg.start()
    await s1.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await s2.push_report(report("cluster", "c2", "s2", 1, name="c2"))
    await wait_for_version(agg, 1)

    await s1.fail()
    await wait_for_source_state(agg, "s1", SourceState.DEAD)
    svc = TopologyService(agg)
    clusters = svc.list_clusters(Scope.all())
    by_id = {e.key.id: e for e in clusters.entities}
    assert by_id["c1"].status.value == "stale"
    assert by_id["c2"].status.value == "active"  # s2 still readable
    await agg.stop()


async def test_stale_clears_on_recovery(make_source):
    s1 = make_source("s1")
    agg = Aggregator([s1])
    await agg.start()
    await s1.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await wait_for_version(agg, 1)

    await s1.fail()
    await wait_for_source_state(agg, "s1", SourceState.DEAD)
    snap = agg.current_snapshot()
    assert snap.source_health["s1"].state is SourceState.DEAD
    assert snap.entities[cluster_key("c1")].status.value == "stale"

    v_dead = agg.version
    await s1.recover()
    await s1.push_report(report("cluster", "c1", "s1", 2, name="c1"))
    await wait_for_source_state(agg, "s1", SourceState.ALIVE)
    await wait_for_version(agg, v_dead + 1)
    snap = agg.current_snapshot()
    assert snap.source_health["s1"].state is SourceState.ALIVE
    assert snap.entities[cluster_key("c1")].status.value == "active"
    await agg.stop()


async def test_heartbeat_keeps_source_alive(make_source):
    """A source sending heartbeats is not marked dead even without reports."""
    s1 = make_source("s1", fail_timeout=0.3)
    agg = Aggregator([s1])
    await agg.start()
    await s1.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await wait_for_version(agg, 1)
    # Keep heartbeating. Here the delay *is* the condition under test
    # (survival past fail_timeout); it is relative to the configured
    # 0.3s timeout, not to machine speed, and the final assertion is
    # synchronized on a concrete published state.
    for _ in range(5):
        await s1.heartbeat()
        await asyncio.sleep(0.1)
    snap = agg.current_snapshot()
    assert snap.source_health["s1"].state is SourceState.ALIVE
    await agg.stop()


async def test_stale_status_is_distinguishable(make_source):
    """Stale vs active status must be explicitly distinguishable in responses."""
    s1 = make_source("s1")
    s2 = make_source("s2")
    agg = Aggregator([s1, s2])
    await agg.start()
    await s1.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await s2.push_report(report("cluster", "c2", "s2", 1, name="c2"))
    await wait_for_version(agg, 1)
    await s2.fail()
    await wait_for_source_state(agg, "s2", SourceState.DEAD)
    svc = TopologyService(agg)
    clusters = svc.list_clusters(Scope.all())
    statuses = {e.key.id: e.status.value for e in clusters.entities}
    assert statuses == {"c1": "active", "c2": "stale"}
    await agg.stop()
