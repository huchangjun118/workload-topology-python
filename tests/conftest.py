"""Shared test fixtures and helpers."""

from __future__ import annotations

import pytest

from topology.aggregator import Aggregator
from topology.model import (
    OrderKey,
    Report,
    cluster_key,
    node_key,
    replica_key,
    workload_key,
)
from topology.sources import DataSource, SourceConfig


@pytest.fixture
def make_source():
    """Factory for in-memory data sources with a short fail timeout."""

    def _make(source_id: str = "s1", fail_timeout: float = 0.4) -> DataSource:
        return DataSource(SourceConfig(source_id, fail_timeout=fail_timeout))

    return _make


@pytest.fixture
async def aggregator(make_source):
    """A started aggregator with a single source, stopped after the test."""
    source = make_source("s1")
    agg = Aggregator([source])
    await agg.start()
    try:
        yield agg, source
    finally:
        await agg.stop()


def report(entity_type: str, entity_id: str, source_id: str, seq: int,
           timestamp_ns: int = 1_000_000_000, **payload) -> Report:
    """Build a Report with a deterministic order key."""
    key = {
        "cluster": cluster_key,
        "node": node_key,
        "workload": workload_key,
        "replica": replica_key,
    }[entity_type](entity_id)
    return Report(
        key=key,
        order=OrderKey(timestamp_ns=timestamp_ns, source_id=source_id, seq=seq),
        payload=dict(payload),
    )


def full_topology(source_id: str = "s1", base_seq: int = 1, ts: int = 1_000_000_000):
    """Return a list of reports forming c1->n1->w1->r1,r2."""
    return [
        report("cluster", "c1", source_id, base_seq, ts, name="cluster-1"),
        report("node", "n1", source_id, base_seq + 1, ts, cluster_id="c1", name="node-1"),
        report("workload", "w1", source_id, base_seq + 2, ts, node_id="n1", name="web"),
        report("replica", "r1", source_id, base_seq + 3, ts, workload_id="w1", node_id="n1", name="web-0"),
        report("replica", "r2", source_id, base_seq + 4, ts, workload_id="w1", node_id="n1", name="web-1"),
    ]


async def push_all(source: DataSource, reports) -> None:
    for r in reports:
        await source.push_report(r)


async def wait_for_version(agg: Aggregator, min_version: int, timeout: float = 2.0) -> None:
    """Wait until the aggregator publishes at least ``min_version``."""
    import asyncio

    deadline = asyncio.get_event_loop().time() + timeout
    while agg.version < min_version:
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(f"version {min_version} never reached (at {agg.version})")
        await asyncio.sleep(0.02)
