"""Shared helpers for topology tests."""

from __future__ import annotations

import random
from typing import Dict, List, Tuple

from topology import (
    Change,
    DataSource,
    Level,
    Node,
    Replica,
    TopologyService,
    TopologyStore,
    Workload,
)


def make_stack():
    store = TopologyStore()
    service = TopologyService(store)
    return store, service


def node_change(node_id: str, cluster: str, version: int, delete: bool = False) -> Change:
    return Change(
        Level.NODE,
        node_id,
        version,
        delete=delete,
        node=None if delete else Node(node_id, cluster),
    )


def workload_change(wl_id: str, node_id: str, version: int, delete: bool = False) -> Change:
    return Change(
        Level.WORKLOAD,
        wl_id,
        version,
        delete=delete,
        workload=None if delete else Workload(wl_id, node_id),
    )


def replica_change(rep_id: str, wl_id: str, version: int, delete: bool = False) -> Change:
    return Change(
        Level.REPLICA,
        rep_id,
        version,
        delete=delete,
        replica=None if delete else Replica(rep_id, wl_id),
    )


def apply_shuffled(store: TopologyStore, batches: List[Tuple[str, int, List[Change]]], seed: int) -> None:
    """Apply (source, seq, changes) batches in a random but valid order.

    Per-source sequence order is preserved (a source never emits out of
    order); only the interleaving across sources is shuffled.
    """
    by_source: Dict[str, List[Tuple[int, List[Change]]]] = {}
    for sid, seq, changes in batches:
        by_source.setdefault(sid, []).append((seq, changes))
    rng = random.Random(seed)
    remaining = {sid: list(items) for sid, items in by_source.items()}
    while remaining:
        sid = rng.choice(list(remaining))
        seq, changes = remaining[sid].pop(0)
        store.apply(sid, seq, changes)
        if not remaining[sid]:
            del remaining[sid]
