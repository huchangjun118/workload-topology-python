"""Concurrency and failure-timing acceptance tests.

Scenarios covered:

* continuous writes while reads are in flight, at increased source and
  entity scale, under read-heavy pressure;
* reads cancelled or timed out mid-flight: no half-built state is ever
  observable and the aggregator keeps making progress afterwards;
* one source fails -> the subtree it solely covers is marked ``stale``;
  when the source recovers, the staleness clears automatically.

Every response must be attributable to one consistent view version.
All synchronization is condition-based (view versions / health states
with a generous deadline) instead of fixed sleeps, so the tests are
stable on slow and fast machines and across repeated runs.
"""

from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from topology.aggregator import Aggregator, Snapshot
from topology.api import create_app
from topology.model import EntityStatus, SourceState
from topology.service import Scope, TopologyService

from .conftest import push_all, report

# Scale parameters: deliberately larger than the unit tests.
NUM_SOURCES = 4
CLUSTERS_PER_SOURCE = 2
NODES_PER_CLUSTER = 6
WORKLOADS_PER_NODE = 3
REPLICAS_PER_WORKLOAD = 2
WRITE_ROUNDS = 40
NUM_READERS = 6

#: Upper bound for condition waits. Never used as a fixed sleep: tests
#: proceed as soon as the condition holds, the deadline only catches
#: genuine hangs.
DEADLINE_S = 15.0

ENTITIES_PER_CLUSTER = 1 + NODES_PER_CLUSTER * (
    1 + WORKLOADS_PER_NODE * (1 + REPLICAS_PER_WORKLOAD)
)


def tree_reports(source_id, cluster_ids, ts=1_000_000_000, seq_start=1):
    """Reports forming full cluster->node->workload->replica trees."""
    reports = []
    seq = seq_start
    for cid in cluster_ids:
        reports.append(report("cluster", cid, source_id, seq, ts, name=cid))
        seq += 1
        for ni in range(NODES_PER_CLUSTER):
            nid = f"{cid}-n{ni}"
            reports.append(report("node", nid, source_id, seq, ts, cluster_id=cid, name=nid))
            seq += 1
            for wi in range(WORKLOADS_PER_NODE):
                wid = f"{nid}-w{wi}"
                reports.append(report("workload", wid, source_id, seq, ts, node_id=nid, name=wid))
                seq += 1
                for ri in range(REPLICAS_PER_WORKLOAD):
                    rid = f"{wid}-r{ri}"
                    reports.append(report("replica", rid, source_id, seq, ts, workload_id=wid, name=rid))
                    seq += 1
    return reports, seq


async def wait_until(pred, description: str, timeout: float = DEADLINE_S) -> None:
    """Poll ``pred`` until it holds; fail with ``description`` on deadline."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not pred():
        if loop.time() > deadline:
            raise TimeoutError(description)
        await asyncio.sleep(0.005)


def assert_snapshot_consistent(snap: Snapshot) -> None:
    """Every edge endpoint must exist in the same snapshot."""
    for parent, edges in snap.edges.items():
        assert parent in snap.entities
        for edge in edges:
            assert edge.child in snap.entities


def assert_view_attributed(view, agg: Aggregator) -> Snapshot:
    """A response must be attributable to one consistent view version."""
    snap = agg.snapshot_at(view.view_version)
    assert snap is not None, f"view version {view.view_version} not in history"
    assert_snapshot_consistent(snap)
    snap_edges = {e for edges in snap.edges.values() for e in edges}
    for entity in view.entities:
        assert entity.key in snap.entities, (
            f"entity {entity.key.id} not in snapshot v{view.view_version}"
        )
        assert snap.entities[entity.key].status == entity.status
    for edge in view.edges:
        assert edge in snap_edges
    return snap


async def test_reads_during_continuous_writes_at_scale(make_source):
    """Read-heavy load while every source keeps writing.

    Every response must be attributable to one consistent view version,
    and the version observed by any single reader must never go backwards.
    """
    sources = [make_source(f"src-{i}") for i in range(NUM_SOURCES)]
    agg = Aggregator(sources, max_history=1000)
    await agg.start()
    try:
        cluster_ids: list[str] = []
        per_source: dict[str, list[str]] = {}
        for i, src in enumerate(sources):
            cids = [f"src{i}-c{j}" for j in range(CLUSTERS_PER_SOURCE)]
            per_source[src.source_id] = cids
            cluster_ids.extend(cids)
            reports, _ = tree_reports(src.source_id, cids)
            await push_all(src, reports)
        await wait_until(
            lambda: len(agg.current_snapshot().entities)
            == NUM_SOURCES * CLUSTERS_PER_SOURCE * ENTITIES_PER_CLUSTER,
            "seed topology was not merged in time",
        )
        svc = TopologyService(agg)
        violations: list[str] = []
        writers_done = asyncio.Event()
        remaining = len(sources)

        async def writer(src, cids) -> None:
            nonlocal remaining
            for r in range(WRITE_ROUNDS):
                cid = cids[r % len(cids)]
                nid = f"{cid}-n{r % NODES_PER_CLUSTER}"
                await src.push_report(
                    report("node", nid, src.source_id, 10_000 + r, 2_000_000_000 + r,
                           cluster_id=cid, name=f"{nid}-v{r}")
                )
                await asyncio.sleep(0)  # let readers interleave
            remaining -= 1
            if remaining == 0:
                writers_done.set()

        async def reader(reader_idx: int) -> int:
            last_version = 0
            reads = 0
            while not writers_done.is_set():
                cid = cluster_ids[(reader_idx + reads) % len(cluster_ids)]
                nodes = svc.list_nodes(Scope.all(), cid)
                if nodes.view_version < last_version:
                    violations.append(
                        f"reader {reader_idx}: version went backwards "
                        f"{last_version} -> {nodes.view_version}"
                    )
                last_version = max(last_version, nodes.view_version)
                try:
                    assert_view_attributed(nodes, agg)
                except AssertionError as exc:
                    violations.append(f"reader {reader_idx}: {exc}")
                # Every entity in the response is reachable via an edge of
                # the same response (no half-applied parent/child state).
                edge_children = {e.child for e in nodes.edges}
                for entity in nodes.entities:
                    if entity.key not in edge_children:
                        violations.append(
                            f"reader {reader_idx}: orphan {entity.key.id} "
                            f"in v{nodes.view_version}"
                        )
                reads += 1
                await asyncio.sleep(0)
            return reads

        writers = [
            asyncio.create_task(writer(src, per_source[src.source_id]))
            for src in sources
        ]
        readers = [asyncio.create_task(reader(k)) for k in range(NUM_READERS)]
        await asyncio.wait_for(asyncio.gather(*writers), timeout=DEADLINE_S)
        read_counts = await asyncio.wait_for(asyncio.gather(*readers), timeout=DEADLINE_S)
        assert violations == []
        assert all(c > 0 for c in read_counts), "readers did not actually run"
        assert_snapshot_consistent(agg.current_snapshot())
    finally:
        await agg.stop()


async def test_cancelled_and_timed_out_reads_leave_no_partial_state(make_source):
    """Cancelling or timing out in-flight reads must not leave half-built
    state: snapshots stay consistent, history stays complete and the
    aggregator keeps making progress."""
    sources = [make_source(f"src-{i}") for i in range(2)]
    agg = Aggregator(sources)
    await agg.start()
    try:
        for src in sources:
            reports, _ = tree_reports(src.source_id, [f"{src.source_id}-c0"])
            await push_all(src, reports)
        await wait_until(
            lambda: len(agg.current_snapshot().entities) == 2 * ENTITIES_PER_CLUSTER,
            "seed topology was not merged in time",
        )
        svc = TopologyService(agg)
        stop_writes = asyncio.Event()

        async def writer() -> None:
            seq = 10_000
            while not stop_writes.is_set():
                await sources[0].push_report(
                    report("node", "src-0-c0-n0", "src-0", seq, 2_000_000_000 + seq,
                           cluster_id="src-0-c0", name=f"v{seq}")
                )
                seq += 1
                await asyncio.sleep(0)

        async def read_loop() -> None:
            while True:
                await asyncio.sleep(0)  # cancellation lands mid read-stream
                svc.list_clusters(Scope.all())
                svc.list_nodes(Scope.all(), "src-0-c0")

        wt = asyncio.create_task(writer())
        # Cancel in-flight reads repeatedly while writes continue.
        for _ in range(20):
            t = asyncio.create_task(read_loop())
            await asyncio.sleep(0)
            t.cancel()
            with pytest.raises(asyncio.CancelledError):
                await t
        # Time out in-flight reads repeatedly (the loop never completes,
        # so wait_for always times out regardless of machine speed).
        for _ in range(20):
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(read_loop(), timeout=0.001)
        stop_writes.set()
        await asyncio.wait_for(wt, timeout=DEADLINE_S)

        # No half-built state: current snapshot and retained history are
        # all internally consistent.
        v_after = agg.version
        assert_snapshot_consistent(agg.current_snapshot())
        recent = [
            s
            for ver in range(max(1, v_after - 20), v_after + 1)
            if (s := agg.snapshot_at(ver)) is not None
        ]
        assert recent, "no recent view version resolves to a snapshot"
        for snap in recent:
            assert_snapshot_consistent(snap)

        # The aggregator still makes progress and serves consistent reads.
        await sources[1].push_report(
            report("cluster", "post-storm", "src-1", 50_000, 9_000_000_000, name="post")
        )
        await wait_until(
            lambda: agg.version > v_after, "aggregator stopped making progress"
        )
        view = svc.list_clusters(Scope.all())
        assert view.view_version == agg.version
        assert "post-storm" in [e.key.id for e in view.entities]
        assert_view_attributed(view, agg)
    finally:
        await agg.stop()


async def test_failed_source_subtree_stale_then_recovers_at_scale(make_source):
    """One source fails among many: its solely-covered subtree goes stale,
    corroborated and foreign entities stay active, and recovery clears
    the staleness automatically."""
    sources = [make_source(f"src-{i}", fail_timeout=0.3) for i in range(NUM_SOURCES)]
    agg = Aggregator(sources)
    await agg.start()
    try:
        per_source: dict[str, list[str]] = {}
        for src in sources:
            cids = [f"{src.source_id}-c{j}" for j in range(CLUSTERS_PER_SOURCE)]
            per_source[src.source_id] = cids
            reports, _ = tree_reports(src.source_id, cids)
            await push_all(src, reports)
        # A cluster corroborated by two sources: must survive either failure.
        await sources[0].push_report(
            report("cluster", "shared-c", "src-0", 90_000, 5_000_000_000, name="shared")
        )
        await sources[1].push_report(
            report("cluster", "shared-c", "src-1", 90_000, 5_000_000_000, name="shared")
        )
        await wait_until(
            lambda: len(agg.current_snapshot().entities)
            == NUM_SOURCES * CLUSTERS_PER_SOURCE * ENTITIES_PER_CLUSTER + 1,
            "seed topology was not merged in time",
        )
        svc = TopologyService(agg)

        victim = sources[1]
        await victim.fail()
        await wait_until(
            lambda: agg.current_snapshot().source_health["src-1"].state
            is SourceState.DEAD,
            "failed source was not marked dead",
        )
        snap = agg.current_snapshot()
        # Stale iff solely covered by the failed source -- nothing else may
        # flip status, even at this scale.
        solely_victim = 0
        for entity in snap.entities.values():
            if entity.contributors == frozenset({"src-1"}):
                solely_victim += 1
                assert entity.status is EntityStatus.STALE, entity.key
            else:
                assert entity.status is EntityStatus.ACTIVE, entity.key
        assert solely_victim > 0, "victim must solely cover part of the tree"
        # The corroborated cluster stays active.
        shared = next(e for e in snap.entities.values() if e.key.id == "shared-c")
        assert shared.status is EntityStatus.ACTIVE
        # Other sources' subtrees remain fully readable and active.
        for cid in per_source["src-2"]:
            nodes = svc.list_nodes(Scope.all(), cid)
            assert len(nodes.entities) == NODES_PER_CLUSTER
            assert all(e.status is EntityStatus.ACTIVE for e in nodes.entities)
            assert_view_attributed(nodes, agg)

        # Recovery clears the staleness automatically: the source resumes
        # heartbeats, liveness flips and the next snapshot is active again.
        await victim.recover()
        await wait_until(
            lambda: agg.current_snapshot().source_health["src-1"].state
            is SourceState.ALIVE,
            "recovered source was not marked alive",
        )
        await wait_until(
            lambda: all(
                e.status is EntityStatus.ACTIVE
                for e in agg.current_snapshot().entities.values()
            ),
            "stale status did not clear after recovery",
        )
    finally:
        await agg.stop()


async def test_api_responses_attributed_to_consistent_view_under_writes(make_source):
    """HTTP reads during continuous writes: every JSON response carries a
    view version that resolves to one consistent snapshot, and a pinned
    drill-down chain stays on the same version."""
    source = make_source("s1")
    agg = Aggregator([source], max_history=1000)
    app = create_app(agg)
    reports, _ = tree_reports("s1", ["c1", "c2"])
    async with app.router.lifespan_context(app):
        await push_all(source, reports)
        await wait_until(
            lambda: len(agg.current_snapshot().entities) == 2 * ENTITIES_PER_CLUSTER,
            "seed topology was not merged in time",
        )
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            stop = asyncio.Event()
            violations: list[str] = []

            async def writer() -> None:
                for r in range(30):
                    nid = f"c1-n{r % NODES_PER_CLUSTER}"
                    await source.push_report(
                        report("node", nid, "s1", 10_000 + r, 2_000_000_000 + r,
                               cluster_id="c1", name=f"{nid}-v{r}")
                    )
                    await asyncio.sleep(0)
                stop.set()

            async def http_reader() -> int:
                reads = 0
                last_version = 0
                while not stop.is_set():
                    r = await client.get("/api/topology/clusters")
                    assert r.status_code == 200
                    body = r.json()
                    v = body["view_version"]
                    snap = agg.snapshot_at(v)
                    if snap is None:
                        violations.append(f"response version {v} not in history")
                        continue
                    expected = {
                        (k.id, e.status.value)
                        for k, e in snap.entities.items()
                        if k.type.value == "cluster"
                    }
                    got = {(e["id"], e["status"]) for e in body["entities"]}
                    if got != expected:
                        violations.append(
                            f"response entities do not match snapshot v{v}"
                        )
                    if v < last_version:
                        violations.append(
                            f"version went backwards {last_version} -> {v}"
                        )
                    last_version = v
                    # Drill down pinned to the same view: must stay on it.
                    r2 = await client.get(f"/api/topology/clusters/c1/nodes?view={v}")
                    assert r2.status_code == 200
                    if r2.json()["view_version"] != v:
                        violations.append("pinned drill-down served another version")
                    reads += 1
                return reads

            wt = asyncio.create_task(writer())
            readers = [asyncio.create_task(http_reader()) for _ in range(4)]
            await asyncio.wait_for(wt, timeout=DEADLINE_S)
            counts = await asyncio.wait_for(asyncio.gather(*readers), timeout=DEADLINE_S)
            assert violations == []
            assert all(c > 0 for c in counts), "HTTP readers did not actually run"
