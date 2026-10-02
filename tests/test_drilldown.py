"""Drill-down levels, dangling-edge freedom, scope filtering, 404 vs 403."""

from __future__ import annotations

import pytest

from topology import (
    ForbiddenError,
    Level,
    Node,
    NotFoundError,
    Scope,
    Change,
    DataSource,
    Health,
)

from .conftest import (
    make_stack,
    node_change,
    replica_change,
    workload_change,
)


@pytest.fixture()
def env():
    """c1: n1(w1: r1,r2), n2(w2: r3); c2: n3(w3: r4)."""
    store, service = make_stack()
    src = DataSource("s", store)
    src.connect()
    src.emit([node_change("n1", "c1", 1), node_change("n2", "c1", 1), node_change("n3", "c2", 1)])
    src.emit([workload_change("w1", "n1", 1), workload_change("w2", "n2", 1), workload_change("w3", "n3", 1)])
    src.emit([
        replica_change("r1", "w1", 1),
        replica_change("r2", "w1", 1),
        replica_change("r3", "w2", 1),
        replica_change("r4", "w3", 1),
    ])
    return store, service


ALL = Scope()


def ids(view):
    return {e.id for e in view.entities}


def test_cluster_level(env):
    _, svc = env
    view = svc.get_level(Level.CLUSTER, ALL)
    assert ids(view) == {"c1", "c2"}
    assert view.edges == ()  # clusters have no parent edges
    assert all(e.parent_id is None for e in view.entities)


def test_node_level_per_cluster(env):
    _, svc = env
    view = svc.get_level(Level.NODE, ALL, "c1")
    assert ids(view) == {"n1", "n2"}
    assert {(e.parent_id, e.child_id) for e in view.edges} == {("c1", "n1"), ("c1", "n2")}
    view2 = svc.get_level(Level.NODE, ALL, "c2")
    assert ids(view2) == {"n3"}


def test_workload_level_per_node(env):
    _, svc = env
    view = svc.get_level(Level.WORKLOAD, ALL, "n1")
    assert ids(view) == {"w1"}
    assert {(e.parent_id, e.child_id) for e in view.edges} == {("n1", "w1")}


def test_replica_level_per_workload(env):
    _, svc = env
    view = svc.get_level(Level.REPLICA, ALL, "w1")
    assert ids(view) == {"r1", "r2"}
    assert {(e.parent_id, e.child_id) for e in view.edges} == {("w1", "r1"), ("w1", "r2")}


def test_no_cross_level_entities_or_dangling_edges(env):
    _, svc = env
    for level, parent in [
        (Level.CLUSTER, None),
        (Level.NODE, "c1"),
        (Level.WORKLOAD, "n1"),
        (Level.REPLICA, "w1"),
    ]:
        view = svc.get_level(level, ALL, parent)
        assert all(e.kind is level for e in view.entities)
        entity_ids = ids(view)
        for edge in view.edges:
            assert edge.child_id in entity_ids  # no dangling child
            if parent is not None:
                assert edge.parent_id == parent  # edge stays within this drill-down


def test_orphan_entities_hidden_when_parent_missing(env):
    store, svc = env
    # source reports a replica whose workload was never reported
    src = DataSource("x", store)
    src.connect()
    src.emit([replica_change("r-orphan", "w-missing", 1)])
    src.emit([workload_change("w-orphan", "n-missing", 1)])
    view = svc.get_level(Level.REPLICA, ALL)
    assert "r-orphan" not in ids(view)
    view = svc.get_level(Level.WORKLOAD, ALL)
    assert "w-orphan" not in ids(view)


def test_scope_filters_entities_and_edges(env):
    _, svc = env
    scope = Scope(clusters=frozenset({"c1"}))
    assert ids(svc.get_level(Level.CLUSTER, scope)) == {"c1"}
    assert ids(svc.get_level(Level.NODE, scope)) == {"n1", "n2"}
    view = svc.get_level(Level.WORKLOAD, scope)
    assert ids(view) == {"w1", "w2"}
    assert all(e.child_id in {"w1", "w2"} for e in view.edges)

    scope = Scope(nodes=frozenset({"n1"}))
    assert ids(svc.get_level(Level.NODE, scope)) == {"n1"}
    assert ids(svc.get_level(Level.WORKLOAD, scope)) == {"w1"}
    assert ids(svc.get_level(Level.REPLICA, scope)) == {"r1", "r2"}

    scope = Scope(workloads=frozenset({"w2"}))
    assert ids(svc.get_level(Level.WORKLOAD, scope)) == {"w2"}
    assert ids(svc.get_level(Level.REPLICA, scope)) == {"r3"}


def test_not_found_vs_forbidden(env):
    _, svc = env
    with pytest.raises(NotFoundError):
        svc.get_level(Level.NODE, ALL, "c-nope")
    with pytest.raises(NotFoundError):
        svc.get_level(Level.WORKLOAD, ALL, "n-nope")
    with pytest.raises(NotFoundError):
        svc.get_level(Level.REPLICA, ALL, "w-nope")

    with pytest.raises(ForbiddenError):
        svc.get_level(Level.NODE, Scope(clusters=frozenset({"c2"})), "c1")
    with pytest.raises(ForbiddenError):
        svc.get_level(Level.WORKLOAD, Scope(nodes=frozenset({"n2"})), "n1")
    with pytest.raises(ForbiddenError):
        svc.get_level(Level.REPLICA, Scope(workloads=frozenset({"w2"})), "w1")


def test_scoped_out_entities_never_leak_into_any_level(env):
    _, svc = env
    scope = Scope(clusters=frozenset({"c2"}))
    for level, parent in [
        (Level.CLUSTER, None),
        (Level.NODE, None),
        (Level.WORKLOAD, None),
        (Level.REPLICA, None),
    ]:
        view = svc.get_level(level, scope, parent)
        leaked = ids(view) & {"n1", "n2", "w1", "w2", "r1", "r2", "r3", "c1"}
        assert not leaked
        for edge in view.edges:
            assert edge.parent_id not in {"c1", "n1", "n2", "w1", "w2"}
            assert edge.child_id not in {"n1", "n2", "w1", "w2", "r1", "r2", "r3"}
