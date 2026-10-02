"""Single-source failure degradation: stale marking, readability of other
sources, automatic recovery on reconnect."""

from __future__ import annotations

from topology import DataSource, Freshness, Level, Scope

from .conftest import make_stack, node_change, replica_change, workload_change

ALL = Scope()


def _build():
    """Two sources, each owning a disjoint subtree of one cluster."""
    store, svc = make_stack()
    a = DataSource("a", store)
    b = DataSource("b", store)
    a.connect()
    b.connect()
    a.emit([node_change("n1", "c1", 1)])
    b.emit([node_change("n2", "c1", 1)])
    a.emit([workload_change("w1", "n1", 1)])
    b.emit([workload_change("w2", "n2", 1)])
    a.emit([replica_change("r1", "w1", 1)])
    b.emit([replica_change("r2", "w2", 1)])
    return store, svc, a, b


def _freshness(view):
    return {e.id: e.freshness for e in view.entities}


def test_failed_source_subtree_marked_stale():
    _, svc, a, b = _build()
    a.disconnect()
    nodes = svc.get_level(Level.NODE, ALL, "c1")
    f = _freshness(nodes)
    assert f["n1"] is Freshness.STALE   # covered by failed source
    assert f["n2"] is Freshness.LIVE    # other source unaffected
    wls = svc.get_level(Level.WORKLOAD, ALL)
    f = _freshness(wls)
    assert f["w1"] is Freshness.STALE
    assert f["w2"] is Freshness.LIVE
    reps = svc.get_level(Level.REPLICA, ALL)
    f = _freshness(reps)
    assert f["r1"] is Freshness.STALE
    assert f["r2"] is Freshness.LIVE


def test_stale_data_remains_readable():
    _, svc, a, _ = _build()
    a.disconnect()
    view = svc.get_level(Level.REPLICA, ALL, "w1")
    assert [e.id for e in view.entities] == ["r1"]  # degraded, not gone
    assert view.entities[0].freshness is Freshness.STALE


def test_source_status_distinguishes_connected():
    _, svc, a, _ = _build()
    a.disconnect()
    view = svc.get_level(Level.CLUSTER, ALL)
    status = {s.source_id: s.connected for s in view.sources}
    assert status == {"a": False, "b": True}


def test_stale_clears_on_recovery():
    _, svc, a, _ = _build()
    a.disconnect()
    assert _freshness(svc.get_level(Level.NODE, ALL))["n1"] is Freshness.STALE
    a.connect()
    assert _freshness(svc.get_level(Level.NODE, ALL))["n1"] is Freshness.LIVE


def test_shared_entity_stale_only_when_all_contributors_down():
    store, svc = make_stack()
    a = DataSource("a", store)
    b = DataSource("b", store)
    a.connect()
    b.connect()
    a.emit([node_change("n1", "c1", 1)])
    b.emit([node_change("n1", "c1", 1)])
    a.disconnect()
    assert _freshness(svc.get_level(Level.NODE, ALL))["n1"] is Freshness.LIVE
    b.disconnect()
    assert _freshness(svc.get_level(Level.NODE, ALL))["n1"] is Freshness.STALE
    a.connect()
    assert _freshness(svc.get_level(Level.NODE, ALL))["n1"] is Freshness.LIVE


def test_cluster_level_aggregates_staleness():
    _, svc, a, _ = _build()
    a.disconnect()
    clusters = _freshness(svc.get_level(Level.CLUSTER, ALL))
    assert clusters["c1"] is Freshness.LIVE  # partially covered -> still live
    # now take down b as well: the whole cluster is stale
    svc2_store = svc._store
    svc2_store.disconnect("b")
    clusters = _freshness(svc.get_level(Level.CLUSTER, ALL))
    assert clusters["c1"] is Freshness.STALE


def test_edges_carry_freshness():
    _, svc, a, _ = _build()
    a.disconnect()
    view = svc.get_level(Level.WORKLOAD, ALL)
    edge_fresh = {(e.parent_id, e.child_id): e.freshness for e in view.edges}
    assert edge_fresh[("n1", "w1")] is Freshness.STALE
    assert edge_fresh[("n2", "w2")] is Freshness.LIVE
