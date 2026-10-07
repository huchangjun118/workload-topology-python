"""Concurrency and failure-timing acceptance tests.

These tests are deliberately stricter than the basic suite:

* every response is attributed to exactly one consistent view version
  and every edge/entity in it is re-verified against that pinned view;
* reads run continuously while a single writer keeps mutating the graph;
* reads are cancelled and timed out mid-flight and must leave no
  half-built state behind;
* a single data source failing marks solely-covered subtrees ``stale``
  while corroborated entities stay ``active``, and recovery clears the
  stale markers automatically;
* the whole set is repeated for several rounds against a topology with
  several sources and a much larger entity count, with many more readers
  than writers (read-heavy pressure).

Synchronization is event-driven (``wait_for_version`` /
``wait_for_source_state``) or bounded by a generous deadline; no test
relies on a fixed wall-clock delay or on machine speed.
"""

from __future__ import annotations

import asyncio

import pytest

from topology.aggregator import Aggregator
from topology.model import (
    EntityType,
    OrderKey,
    Report,
    SourceState,
    cluster_key,
    node_key,
    workload_key,
)
from topology.service import Scope, TopologyService
from topology.sources import DataSource, SourceConfig

from .conftest import report, wait_for_source_state, wait_for_version

#: Run the whole pressure scenario this many times in-process.
ROUNDS = 5
#: Scale: clearly larger than the handful of entities in the basic tests.
N_CLUSTERS = 6
N_NODES_PER_CLUSTER = 6
N_WORKLOADS_PER_NODE = 8
N_SOURCES = 4


# ---------------------------------------------------------------------------
# topology builders and invariants
# ---------------------------------------------------------------------------


def build_scaled_sources(n_sources: int = N_SOURCES) -> list[DataSource]:
    return [
        DataSource(
            SourceConfig(
                source_id=f"s{i}",
                fail_timeout=0.4,
                heartbeat_interval=0.05,
            )
        )
        for i in range(n_sources)
    ]


def scaled_topology_reports(n_clusters: int = N_CLUSTERS):
    """Yield (source_id, Report) pairs for a sizeable complete hierarchy.

    Each cluster is owned by exactly one source (round-robin), so that
    source's failure flips its whole cluster subtree to stale.
    """
    seq = {}
    ts = 5_000_000_000

    def order(source_id: str) -> OrderKey:
        n = seq.get(source_id, 0) + 1
        seq[source_id] = n
        return OrderKey(ts, source_id, n)

    for c in range(n_clusters):
        cid = f"c{c}"
        owner = f"s{c % N_SOURCES}"
        yield owner, Report(cluster_key(cid), order(owner), {"name": cid})
        for n in range(N_NODES_PER_CLUSTER):
            nid = f"{cid}-n{n}"
            yield owner, Report(node_key(nid), order(owner), {"cluster_id": cid})
            for w in range(N_WORKLOADS_PER_NODE):
                wid = f"{nid}-w{w}"
                yield owner, Report(workload_key(wid), order(owner), {"node_id": nid})


# ---------------------------------------------------------------------------
# invariants
# ---------------------------------------------------------------------------


def _check_layer_attribution(svc: TopologyService, agg: Aggregator,
                             violations: list[str]) -> None:
    """Read all layers and verify each response is one consistent view."""
    clusters = svc.list_clusters(Scope.all())
    if not _view_is_self_consistent(agg, clusters, violations):
        return
    for cluster in clusters.entities:
        cid = cluster.key.id
        nodes = svc.list_nodes(Scope.all(), cid)
        if not _view_is_self_consistent(agg, nodes, violations):
            continue
        for node in nodes.entities:
            workloads = svc.list_workloads(Scope.all(), node.key.id)
            if not _view_is_self_consistent(agg, workloads, violations):
                continue


def _view_is_self_consistent(agg: Aggregator, view, violations: list[str]) -> bool:
    snap = agg.snapshot_at(view.view_version)
    if snap is None:
        violations.append(
            f"{view.layer.value} response references unknown version "
            f"{view.view_version} (latest={agg.version})"
        )
        return False
    if snap.version != view.view_version:
        violations.append("response version does not match pinned snapshot")
        return False
    for entity in view.entities:
        snap_entity = snap.entities.get(entity.key)
        if snap_entity is None:
            violations.append(
                f"entity {entity.key.id} absent from its response version "
                f"{view.view_version}"
            )
            continue
        if snap_entity != entity:
            violations.append(
                f"entity {entity.key.id} differs from pinned version "
                f"{view.view_version}"
            )
    edge_children = {e.child for e in view.edges}
    entity_keys = {e.key for e in view.entities}
    # The cluster layer has no parent->child edges by contract; every
    # other layer must return exactly one edge per returned child.
    if view.layer is not EntityType.CLUSTER and edge_children != entity_keys:
        violations.append(
            f"{view.layer.value} v{view.view_version}: edges/entities mismatch "
            f"(edges={len(edge_children)}, entities={len(entity_keys)})"
        )
    for edge in view.edges:
        if edge.parent not in snap.entities or edge.child not in snap.entities:
            violations.append(
                f"dangling edge in v{view.view_version}: "
                f"{edge.parent.id}->{edge.child.id}"
            )
    return not violations


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("round_no", range(ROUNDS))
async def test_read_heavy_pressure_with_continuous_writer(
    make_source, round_no
):
    """Many readers + one writer: every read is one consistent view."""
    sources = build_scaled_sources()
    source_by_id = {s.source_id: s for s in sources}
    agg = Aggregator(sources, max_history=4000)
    await agg.start()
    try:
        for sid, rep in scaled_topology_reports():
            await source_by_id[sid].push_report(rep)
        await wait_for_version(agg, 1)
        svc = TopologyService(agg)

        stop = asyncio.Event()
        violations: list[str] = []
        write_rounds = 0
        target_write_rounds = 40
        n_readers = 8

        async def writer():
            nonlocal write_rounds
            # Mutates node payloads in place: each round bumps versions
            # but keeps the hierarchy complete (no edges go dangling).
            while not stop.is_set():
                owner = f"s{write_rounds % N_SOURCES}"
                cid = f"c{write_rounds % N_CLUSTERS}"
                nid = f"{cid}-n{write_rounds % N_NODES_PER_CLUSTER}"
                await source_by_id[owner].push_report(
                    report(
                        "node",
                        nid,
                        owner,
                        10_000 + write_rounds,
                        timestamp_ns=6_000_000_000 + write_rounds,
                        cluster_id=cid,
                        rev=write_rounds,
                    )
                )
                write_rounds += 1
                await asyncio.sleep(0)

        async def reader():
            while not stop.is_set():
                _check_layer_attribution(svc, agg, violations)
                await asyncio.sleep(0)

        readers = [asyncio.create_task(reader()) for _ in range(n_readers)]
        wt = asyncio.create_task(writer())
        try:
            while write_rounds < target_write_rounds and not violations:
                await asyncio.sleep(0)
            stop.set()
            await wt
            await asyncio.gather(*readers)
        finally:
            stop.set()
        assert violations == []
        assert write_rounds >= target_write_rounds
    finally:
        await agg.stop()


async def test_cancelled_in_flight_read_leaves_consistent_state(make_source):
    """Cancel a read that is parked mid-flight; state stays consistent."""
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    try:
        for sid, rep in scaled_topology_reports(n_clusters=2):
            await source.push_report(rep)
        await wait_for_version(agg, 1)
        svc = TopologyService(agg)

        entered = asyncio.Event()
        release = asyncio.Event()

        async def parked_read():
            entered.set()
            await release.wait()  # cancelled while parked here
            return svc.list_clusters(Scope.all())

        task = asyncio.create_task(parked_read())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.done() and task.cancelled()

        # New writes still publish complete snapshots after the cancellation.
        v = agg.version
        await source.push_report(
            report("node", "c0-n0", "s1", 99_001, timestamp_ns=7_000_000_001,
                   cluster_id="c0", after_cancel=True)
        )
        await wait_for_version(agg, v + 1)
        view = svc.list_nodes(Scope.all(), "c0")
        violations: list[str] = []
        assert _view_is_self_consistent(agg, view, violations)
        assert violations == []
        assert any(
            e.payload.get("after_cancel") is True for e in view.entities
        )
    finally:
        await agg.stop()


async def test_timed_out_read_leaves_consistent_state(make_source):
    """A wait_for timeout must behave like cancellation: no residue."""
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    try:
        for sid, rep in scaled_topology_reports(n_clusters=2):
            await source.push_report(rep)
        await wait_for_version(agg, 1)
        svc = TopologyService(agg)

        async def parked_read():
            await asyncio.sleep(10)
            return svc.list_clusters(Scope.all())

        parked = asyncio.create_task(parked_read())
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(parked, timeout=0.05)

        # The read task itself was cancelled and is fully done: no
        # half-finished read left in flight.
        assert parked.done()
        assert parked.cancelled()
        v = agg.version
        await source.push_report(
            report("workload", "c0-n0-w0", "s1", 99_002,
                   timestamp_ns=7_000_000_002, node_id="c0-n0",
                   after_timeout=True)
        )
        await wait_for_version(agg, v + 1)
        view = svc.list_workloads(Scope.all(), "c0-n0")
        violations: list[str] = []
        assert _view_is_self_consistent(agg, view, violations)
        assert any(
            e.payload.get("after_timeout") is True for e in view.entities
        )
    finally:
        await agg.stop()


async def test_wait_for_version_itself_cancellable(make_source):
    """Cancelling a waiter on a view version must not poison the aggregator."""
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    try:
        await source.push_report(report("cluster", "c1", "s1", 1, name="c1"))
        await wait_for_version(agg, 1)

        task = asyncio.create_task(agg.wait_for_version(999, timeout=30))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # A later waiter and later publications both keep working.
        await source.push_report(report("node", "n1", "s1", 2, cluster_id="c1"))
        await agg.wait_for_version(2)
        svc = TopologyService(agg)
        assert [e.key.id for e in svc.list_nodes(Scope.all(), "c1").entities] == ["n1"]
    finally:
        await agg.stop()


@pytest.mark.parametrize("round_no", range(ROUNDS))
async def test_failure_marks_subtree_stale_and_recovery_clears(
    make_source, round_no
):
    """One source fails -> its sole subtrees stale; recovery auto-clears."""
    sources = build_scaled_sources()
    source_by_id = {s.source_id: s for s in sources}
    agg = Aggregator(sources, max_history=2000)
    await agg.start()
    try:
        for sid, rep in scaled_topology_reports():
            await source_by_id[sid].push_report(rep)
        await wait_for_version(agg, 1)
        svc = TopologyService(agg)

        # Everything starts active.
        snap = agg.current_snapshot()
        assert all(h.state is SourceState.ALIVE for h in snap.source_health.values())
        clusters = svc.list_clusters(Scope.all())
        assert all(e.status.value == "active" for e in clusters.entities)

        # Fail s0: clusters owned solely by s0 go stale; other owners active.
        victim = source_by_id["s0"]
        owned_cluster_ids = {f"c{c}" for c in range(N_CLUSTERS) if c % N_SOURCES == 0}
        await victim.fail()
        await wait_for_source_state(agg, "s0", SourceState.DEAD, timeout=3.0)

        clusters = svc.list_clusters(Scope.all())
        status = {e.key.id: e.status.value for e in clusters.entities}
        for cid, st in status.items():
            if cid in owned_cluster_ids:
                assert st == "stale", cid
            else:
                assert st == "active", cid

        # Stale data is still fully readable (drill-down stays consistent).
        for cid in owned_cluster_ids:
            nodes = svc.list_nodes(Scope.all(), cid)
            violations: list[str] = []
            assert _view_is_self_consistent(agg, nodes, violations)
            assert violations == []
            assert len(nodes.entities) == N_NODES_PER_CLUSTER
            assert all(e.status.value == "stale" for e in nodes.entities)

        # Recover with a fresh report: stale markers clear automatically.
        v_before = agg.version
        await victim.recover()
        await source_by_id["s0"].push_report(
            report("cluster", next(iter(owned_cluster_ids)), "s0", 88_000,
                   timestamp_ns=8_000_000_000, name="recovered")
        )
        await wait_for_source_state(agg, "s0", SourceState.ALIVE, timeout=3.0)
        await agg.wait_for_version(v_before + 1, timeout=3.0)

        # Heartbeats from the recovered source make all its entities active
        # again; wait specifically for the all-active publication so the
        # assertion never races the recovery heartbeat.
        def all_active(snap) -> bool:
            health = snap.source_health.get("s0")
            if health is None or health.state is not SourceState.ALIVE:
                return False
            return all(
                ent.status.value == "active"
                for ent in snap.entities.values()
                if ent.key.type is EntityType.CLUSTER
            )

        assert await agg.wait_until(all_active, timeout=3.0)
        clusters = svc.list_clusters(Scope.all())
        assert all(e.status.value == "active" for e in clusters.entities)
    finally:
        await agg.stop()
