"""Tests for caller scope filtering and 403 vs 404 distinction."""

from __future__ import annotations

import asyncio

from topology.aggregator import Aggregator
from topology.service import ForbiddenError, NotFoundError, Scope, TopologyService

from .conftest import full_topology, push_all, report


async def _service(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await push_all(source, full_topology("s1"))
    await asyncio.sleep(0.2)
    return agg, TopologyService(agg)


async def test_scope_filters_clusters(make_source):
    agg, svc = await _service(make_source)
    # Add a second cluster.
    source = agg._sources["s1"]
    await source.push_report(report("cluster", "c2", "s1", 10, name="c2"))
    await asyncio.sleep(0.15)

    view = svc.list_clusters(Scope(visible_clusters=frozenset({"c1"})))
    assert [e.key.id for e in view.entities] == ["c1"]
    await agg.stop()


async def test_scope_filters_entire_subtree(make_source):
    agg, svc = await _service(make_source)
    # c2 subtree
    source = agg._sources["s1"]
    await source.push_report(report("cluster", "c2", "s1", 10, name="c2"))
    await source.push_report(report("node", "n3", "s1", 11, cluster_id="c2"))
    await source.push_report(report("workload", "w3", "s1", 12, node_id="n3"))
    await source.push_report(report("replica", "r4", "s1", 13, workload_id="w3"))
    await asyncio.sleep(0.15)

    scope = Scope(visible_clusters=frozenset({"c1"}))
    clusters = svc.list_clusters(scope)
    assert [e.key.id for e in clusters.entities] == ["c1"]
    nodes = svc.list_nodes(scope, "c1")
    assert [e.key.id for e in nodes.entities] == ["n1"]
    workloads = svc.list_workloads(scope, "n1")
    assert [e.key.id for e in workloads.entities] == ["w1"]
    replicas = svc.list_replicas(scope, "w1")
    assert [e.key.id for e in replicas.entities] == ["r1", "r2"]
    # No cross-layer edges leak in.
    assert all(e.child.id in {"n1"} for e in nodes.edges)
    await agg.stop()


async def test_forbidden_vs_not_found(make_source):
    agg, svc = await _service(make_source)
    # c1 exists but caller scope is c2 -> 403
    try:
        svc.list_nodes(Scope(visible_clusters=frozenset({"c2"})), "c1")
    except ForbiddenError:
        pass
    else:
        raise AssertionError("expected ForbiddenError")

    # cX does not exist -> 404
    try:
        svc.list_nodes(Scope.all(), "cX")
    except NotFoundError:
        pass
    else:
        raise AssertionError("expected NotFoundError")

    # Node n1 exists but scope c2 -> 403
    try:
        svc.list_workloads(Scope(visible_clusters=frozenset({"c2"})), "n1")
    except ForbiddenError:
        pass
    else:
        raise AssertionError("expected ForbiddenError")

    # Workload wX does not exist -> 404
    try:
        svc.list_replicas(Scope.all(), "wX")
    except NotFoundError:
        pass
    else:
        raise AssertionError("expected NotFoundError")
    await agg.stop()


async def test_scope_all_sees_everything(make_source):
    agg, svc = await _service(make_source)
    view = svc.list_clusters(Scope.all())
    assert [e.key.id for e in view.entities] == ["c1"]
    nodes = svc.list_nodes(Scope.all(), "c1")
    assert [e.key.id for e in nodes.entities] == ["n1"]
    await agg.stop()


async def test_empty_scope_sees_nothing(make_source):
    agg, svc = await _service(make_source)
    view = svc.list_clusters(Scope(visible_clusters=frozenset()))
    assert view.entities == ()
    try:
        svc.list_nodes(Scope(visible_clusters=frozenset()), "c1")
    except ForbiddenError:
        pass
    else:
        raise AssertionError("expected ForbiddenError")
    await agg.stop()


async def test_pinned_view_is_consistent(make_source):
    agg, svc = await _service(make_source)
    v1 = agg.version
    pinned = svc.list_clusters(Scope.all(), view=v1)
    assert pinned.view_version == v1

    # Push a change; the pinned view must remain unchanged.
    source = agg._sources["s1"]
    await source.push_report(report("cluster", "c2", "s1", 10, name="c2"))
    await asyncio.sleep(0.15)
    pinned_after = svc.list_clusters(Scope.all(), view=v1)
    assert pinned_after.view_version == v1
    assert [e.key.id for e in pinned_after.entities] == ["c1"]
    await agg.stop()


async def test_stale_entity_still_returned_with_status(make_source):
    """Stale entities are readable but marked; scope still applies."""
    s1 = make_source("s1")
    s2 = make_source("s2")
    agg = Aggregator([s1, s2])
    await agg.start()
    await s1.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await s2.push_report(report("node", "n1", "s2", 1, cluster_id="c1"))
    await asyncio.sleep(0.2)
    await s2.fail()
    await asyncio.sleep(0.5)
    svc = TopologyService(agg)
    nodes = svc.list_nodes(Scope.all(), "c1")
    assert len(nodes.entities) == 1
    assert nodes.entities[0].status.value == "stale"
    await agg.stop()
