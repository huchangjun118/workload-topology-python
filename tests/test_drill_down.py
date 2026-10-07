"""Tests for hierarchical drill-down and edge consistency."""

from __future__ import annotations


from topology.aggregator import Aggregator
from topology.model import EntityType
from topology.service import Scope, TopologyService

from .conftest import full_topology, push_all, report, wait_for_version


async def test_four_layers(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await push_all(source, full_topology("s1"))
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)

    clusters = svc.list_clusters(Scope.all())
    assert clusters.layer is EntityType.CLUSTER
    assert [e.key.id for e in clusters.entities] == ["c1"]
    assert clusters.edges == ()
    assert clusters.view_version == agg.version

    nodes = svc.list_nodes(Scope.all(), "c1")
    assert [e.key.id for e in nodes.entities] == ["n1"]
    assert len(nodes.edges) == 1
    edge = nodes.edges[0]
    assert edge.parent.id == "c1" and edge.child.id == "n1"

    workloads = svc.list_workloads(Scope.all(), "n1")
    assert [e.key.id for e in workloads.entities] == ["w1"]
    assert workloads.edges[0].parent.id == "n1"
    assert workloads.edges[0].child.id == "w1"

    replicas = svc.list_replicas(Scope.all(), "w1")
    assert [e.key.id for e in replicas.entities] == ["r1", "r2"]
    assert {e.child.id for e in replicas.edges} == {"r1", "r2"}
    assert all(e.parent.id == "w1" for e in replicas.edges)
    await agg.stop()


async def test_no_dangling_edges_for_missing_parent(make_source):
    """A workload whose node is missing must not appear (no dangling edge)."""
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    # cluster c1 exists, but workload w1 points to a missing node nX.
    await source.push_report(report("cluster", "c1", "s1", 1, name="c1"))
    await source.push_report(report("workload", "w1", "s1", 2, node_id="nX", name="web"))
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)

    # nX does not exist -> 404, and w1 is not reachable through any layer.
    nodes = svc.list_nodes(Scope.all(), "c1")
    assert nodes.entities == ()
    assert nodes.edges == ()
    await agg.stop()


async def test_orphan_node_not_returned(make_source):
    """A node whose cluster is missing is an orphan and never returned."""
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    # Node n1 points to cluster cX which is never reported.
    await source.push_report(report("node", "n1", "s1", 1, cluster_id="cX"))
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)
    # cX is not a cluster entity -> 404.
    from topology.service import NotFoundError

    try:
        svc.list_nodes(Scope.all(), "cX")
    except NotFoundError:
        pass
    else:
        raise AssertionError("expected NotFoundError")
    await agg.stop()


async def test_edges_are_direct_parent_child_only(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    await push_all(source, full_topology("s1"))
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)

    # Node layer edges must only connect cluster->node, never cluster->workload.
    nodes = svc.list_nodes(Scope.all(), "c1")
    for edge in nodes.edges:
        assert edge.parent.type is EntityType.CLUSTER
        assert edge.child.type is EntityType.NODE

    workloads = svc.list_workloads(Scope.all(), "n1")
    for edge in workloads.edges:
        assert edge.parent.type is EntityType.NODE
        assert edge.child.type is EntityType.WORKLOAD

    replicas = svc.list_replicas(Scope.all(), "w1")
    for edge in replicas.edges:
        assert edge.parent.type is EntityType.WORKLOAD
        assert edge.child.type is EntityType.REPLICA
    await agg.stop()


async def test_multiple_clusters_and_nodes(make_source):
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    reports = [
        report("cluster", "c1", "s1", 1, name="c1"),
        report("cluster", "c2", "s1", 2, name="c2"),
        report("node", "n1", "s1", 3, cluster_id="c1"),
        report("node", "n2", "s1", 4, cluster_id="c1"),
        report("node", "n3", "s1", 5, cluster_id="c2"),
    ]
    await push_all(source, reports)
    await wait_for_version(agg, 1)
    svc = TopologyService(agg)

    clusters = svc.list_clusters(Scope.all())
    assert [e.key.id for e in clusters.entities] == ["c1", "c2"]
    c1_nodes = svc.list_nodes(Scope.all(), "c1")
    assert [e.key.id for e in c1_nodes.entities] == ["n1", "n2"]
    c2_nodes = svc.list_nodes(Scope.all(), "c2")
    assert [e.key.id for e in c2_nodes.entities] == ["n3"]
    await agg.stop()
