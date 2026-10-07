"""Tests for deterministic merge, duplicate folding and tombstones."""

from __future__ import annotations

import asyncio

from topology.aggregator import Aggregator
from topology.model import EntityKey, EntityType, OrderKey, Report, node_key
from topology.service import Scope, TopologyService

from .conftest import push_all, report, wait_for_version


async def test_merge_is_order_independent(make_source):
    """Same events in different arrival orders produce the same graph."""
    s1 = make_source("s1")
    s2 = make_source("s2")

    events = [
        report("cluster", "c1", "s1", 1, name="c1"),
        report("node", "n1", "s1", 2, cluster_id="c1", name="n1-old"),
        report("node", "n1", "s2", 1, cluster_id="c1", name="n1-new"),
        report("node", "n2", "s2", 2, cluster_id="c1", name="n2"),
    ]

    async def build(shuffled: bool):
        sources = [make_source("s1"), make_source("s2")]
        agg = Aggregator(sources)
        await agg.start()
        ordered = list(events)
        if shuffled:
            ordered = [events[3], events[1], events[0], events[2]]
        for ev in ordered:
            src = sources[0] if ev.order.source_id == "s1" else sources[1]
            await src.push_report(ev)
        await wait_for_version(agg, 1)
        snap = agg.current_snapshot()
        await agg.stop()
        return snap

    snap_a = await build(shuffled=False)
    snap_b = await build(shuffled=True)

    assert snap_a.entities.keys() == snap_b.entities.keys()
    for key in snap_a.entities:
        assert snap_a.entities[key].payload == snap_b.entities[key].payload
        assert snap_a.entities[key].status == snap_b.entities[key].status
    # s2's higher order key wins regardless of arrival order
    assert snap_a.entities[node_key("n1")].payload["name"] == "n1-new"
    assert snap_b.entities[node_key("n1")].payload["name"] == "n1-new"


async def test_duplicate_events_are_folded(aggregator):
    agg, source = aggregator
    await push_all(source, [report("cluster", "c1", "s1", 1, name="c1")])
    await wait_for_version(agg, 1)
    v1 = agg.version

    # Re-send the exact same report (same order key) -> folded, no new version.
    await source.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await asyncio.sleep(0.05)
    assert agg.version == v1

    # Re-send with a newer order key -> applied.
    await source.push_report(report("cluster", "c1", "s1", 2, name="c1-renamed"))
    await wait_for_version(agg, v1 + 1)
    assert agg.version == v1 + 1
    assert agg.current_snapshot().entities[EntityKey(EntityType.CLUSTER, "c1")].payload["name"] == "c1-renamed"


async def test_out_of_order_does_not_roll_back(aggregator):
    agg, source = aggregator
    # Newer report arrives first.
    await source.push_report(report("node", "n1", "s1", 10, cluster_id="c1", name="new"))
    await wait_for_version(agg, 1)
    v_new = agg.version
    # Older report arrives late -> must be ignored, no rollback.
    await source.push_report(report("node", "n1", "s1", 1, cluster_id="c1", name="old"))
    await asyncio.sleep(0.05)
    assert agg.version == v_new
    assert agg.current_snapshot().entities[node_key("n1")].payload["name"] == "new"


async def test_tombstone_deletes_and_old_report_cannot_resurrect(aggregator):
    agg, source = aggregator
    await source.push_report(report("node", "n1", "s1", 1, cluster_id="c1"))
    await wait_for_version(agg, 1)
    assert node_key("n1") in agg.current_snapshot().entities

    # Delete with a newer order key.
    await source.push_report(
        Report(node_key("n1"), OrderKey(2_000_000_000, "s1", 2), {}, deleted=True)
    )
    await wait_for_version(agg, 2)
    assert node_key("n1") not in agg.current_snapshot().entities

    # An old report arriving after the tombstone cannot resurrect.
    await source.push_report(report("node", "n1", "s1", 1, cluster_id="c1"))
    await asyncio.sleep(0.05)
    assert node_key("n1") not in agg.current_snapshot().entities

    # A genuinely newer report (higher order key than the tombstone) can re-create.
    await source.push_report(
        report("node", "n1", "s1", 3, timestamp_ns=3_000_000_000, cluster_id="c1", name="reborn")
    )
    await wait_for_version(agg, 3)
    assert node_key("n1") in agg.current_snapshot().entities
    assert agg.current_snapshot().entities[node_key("n1")].payload["name"] == "reborn"


async def test_contributors_tracked_per_entity(make_source):
    s1 = make_source("s1")
    s2 = make_source("s2")
    agg = Aggregator([s1, s2])
    await agg.start()
    await s1.push_report(report("node", "n1", "s1", 1, cluster_id="c1"))
    await s2.push_report(report("node", "n1", "s2", 1, cluster_id="c1"))
    await wait_for_version(agg, 1)
    entity = agg.current_snapshot().entities[node_key("n1")]
    assert entity.contributors == frozenset({"s1", "s2"})
    await agg.stop()


async def test_view_version_is_monotonic(aggregator):
    agg, source = aggregator
    versions = []
    for i in range(5):
        await source.push_report(report("node", f"n{i}", "s1", i + 1, cluster_id="c1"))
        # Wait for each individual report to be published as its own
        # version; the test asserts a version per change, so it must
        # synchronize on publication instead of racing the batch drain.
        await wait_for_version(agg, i + 1)
        versions.append(agg.version)
    assert versions == sorted(versions)
    assert len(set(versions)) == len(versions)
