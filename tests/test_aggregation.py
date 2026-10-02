"""Aggregation & merge semantics: multi-source merge, order independence,
out-of-order / duplicate folding, delete propagation, reconnect catch-up."""

from __future__ import annotations

from topology import DataSource, Health, Level, Node

from .conftest import (
    apply_shuffled,
    make_stack,
    node_change,
    replica_change,
    workload_change,
)


def test_single_source_ingest():
    store, _ = make_stack()
    src = DataSource("s1", store)
    src.connect()
    src.emit([node_change("n1", "c1", 1)])
    merged = store.merged_nodes()
    assert set(merged) == {"n1"}
    node, sources, stale, version = merged["n1"]
    assert node.cluster_id == "c1"
    assert sources == ("s1",)
    assert stale is False
    assert version == 1


def test_same_entity_two_sources_merges_deterministically():
    # both sources report n1; highest version wins regardless of arrival order
    outcomes = []
    for seed in range(5):
        store, _ = make_stack()
        store.connect("a")
        store.connect("b")
        batches = [
            ("a", 1, [node_change("n1", "c1", 3)]),
            ("b", 1, [node_change("n1", "c1", 5)]),
            ("a", 2, [node_change("n1", "c1", 4)]),
        ]
        apply_shuffled(store, batches, seed)
        merged = store.merged_nodes()
        outcomes.append(merged["n1"])
    assert all(o == outcomes[0] for o in outcomes)
    _, sources, _, version = outcomes[0]
    assert version == 5  # b's record wins: highest version
    assert sources == ("a", "b")  # both live records are contributors


def test_version_tie_broken_by_source_id():
    results = []
    for order in (("a", "b"), ("b", "a")):
        store, _ = make_stack()
        store.connect("a")
        store.connect("b")
        for i, sid in enumerate(order):
            store.apply(sid, 1, [node_change("n1", f"c-from-{sid}", 7)])
        results.append(store.merged_nodes()["n1"][0].cluster_id)
    assert results == ["c-from-a", "c-from-a"]  # lexicographically smaller source wins


def test_out_of_order_versions_do_not_regress():
    store, _ = make_stack()
    store.connect("a")
    store.apply("a", 1, [node_change("n1", "c1", 10)])
    store.apply("a", 2, [node_change("n1", "c1", 5)])  # stale version in newer batch
    _, _, _, version = store.merged_nodes()["n1"]
    assert version == 10


def test_duplicate_batches_are_folded():
    store, _ = make_stack()
    store.connect("a")
    v1 = store.apply("a", 1, [node_change("n1", "c1", 1)])
    v2 = store.apply("a", 1, [node_change("n1", "c1", 1)])  # exact replay
    v3 = store.apply("a", 1, [node_change("n1", "c1", 99)])  # same seq, must be ignored
    assert v1 == v2 == v3
    _, _, _, version = store.merged_nodes()["n1"]
    assert version == 1


def test_delete_requires_all_sources():
    store, _ = make_stack()
    store.connect("a")
    store.connect("b")
    store.apply("a", 1, [node_change("n1", "c1", 1)])
    store.apply("b", 1, [node_change("n1", "c1", 1)])
    store.apply("a", 2, [node_change("n1", "c1", 2, delete=True)])
    assert "n1" in store.merged_nodes()  # b still reports it
    store.apply("b", 2, [node_change("n1", "c1", 2, delete=True)])
    assert "n1" not in store.merged_nodes()  # gone only when all sources deleted it


def test_reconnect_replays_buffered_changes():
    store, _ = make_stack()
    src = DataSource("a", store)
    src.connect()
    src.emit([node_change("n1", "c1", 1)])
    src.disconnect()
    src.emit([node_change("n1", "c1", 2)])
    src.emit([workload_change("w1", "n1", 1)])
    assert "w1" not in store.merged_workloads()  # buffered while disconnected
    src.connect()  # catch-up replay
    assert "w1" in store.merged_workloads()
    _, _, _, version = store.merged_nodes()["n1"]
    assert version == 2


def test_full_topology_aggregation():
    store, _ = make_stack()
    a = DataSource("a", store)
    b = DataSource("b", store)
    a.connect()
    b.connect()
    a.emit([node_change("n1", "c1", 1), node_change("n2", "c1", 1)])
    b.emit([node_change("n3", "c2", 1)])
    a.emit([workload_change("w1", "n1", 1), workload_change("w2", "n2", 1)])
    b.emit([workload_change("w3", "n3", 1)])
    a.emit([replica_change("r1", "w1", 1), replica_change("r2", "w1", 1)])
    assert set(store.merged_nodes()) == {"n1", "n2", "n3"}
    assert set(store.merged_workloads()) == {"w1", "w2", "w3"}
    assert set(store.merged_replicas()) == {"r1", "r2"}
