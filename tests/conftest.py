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


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Treat skipped tests as failures.

    The acceptance criteria require every collected test to actually
    execute. A skip (including an async test that pytest would collect
    but not await if its async plugin were missing) must therefore fail
    the run loudly instead of being reported as a quiet "s" / "passed".
    """
    outcome = yield
    report = outcome.get_result()
    if call.when in ("setup", "call") and report.skipped:
        reason = str(getattr(call.excinfo, "value", "") or "unknown reason")
        report.outcome = "failed"
        report.longrepr = (
            str(item.fspath),
            0,
            f"test was skipped, but this project does not allow silent skips "
            f"(check the pytest-asyncio setup): {reason}",
        )
        report.wasxfail = None


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


async def wait_for_version(agg: Aggregator, min_version: int, timeout: float = 5.0) -> None:
    """Wait until the aggregator publishes at least ``min_version``.

    Backed by the aggregator's condition variable: it wakes only on real
    publications, so no fixed wall-clock sleeps or machine-speed
    assumptions are involved. The generous timeout is a safety net only.
    """
    await agg.wait_for_version(min_version, timeout=timeout)


async def wait_for_source_state(
    agg: Aggregator, source_id: str, state, timeout: float = 5.0
) -> None:
    """Wait until a source's published health reaches ``state``."""
    await agg.wait_for_source_state(source_id, state, timeout=timeout)
