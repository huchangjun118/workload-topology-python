"""Demo topology: builds in-memory data sources and sample reports.

Used by ``main.py`` to run a self-contained service without any external
cluster, credential or network.
"""

from __future__ import annotations

from .model import (
    OrderKey,
    Report,
    cluster_key,
    node_key,
    replica_key,
    workload_key,
)
from .sources import DataSource, SourceConfig


def build_demo_sources() -> list[DataSource]:
    """Create the demo data sources (no reports pushed yet).

    * ``source-a`` covers cluster ``c1``.
    * ``source-b`` covers cluster ``c2``.
    """
    return [
        DataSource(
            SourceConfig(
                source_id="source-a",
                name="demo source A",
                scope=frozenset({"c1"}),
            )
        ),
        DataSource(
            SourceConfig(
                source_id="source-b",
                name="demo source B",
                scope=frozenset({"c2"}),
            )
        ),
    ]


async def seed_demo_reports(sources: list[DataSource]) -> None:
    """Push the initial demo topology into the source pipes.

    Topology::

        c1 (cluster)
          n1 (node)
            w1 (workload)
              r1, r2 (replicas)
          n2 (node)
            w2 (workload)
              r3 (replica)
        c2 (cluster)
          n3 (node)
            w3 (workload)
              r4 (replica)
    """
    by_id = {s.source_id: s for s in sources}
    a = by_id["source-a"]
    b = by_id["source-b"]

    ts = 1_700_000_000_000_000_000  # fixed base timestamp (ns)

    async def push(source: DataSource, seq: int, report: Report) -> None:
        await source.push_report(report)

    # source-a reports cluster c1 and its whole subtree
    seq = 1
    await push(a, seq, Report(cluster_key("c1"), OrderKey(ts, "source-a", seq), {"name": "cluster-1"}))
    seq += 1
    await push(a, seq, Report(node_key("n1"), OrderKey(ts, "source-a", seq), {"cluster_id": "c1", "name": "node-1", "capacity": "4c8g"}))
    seq += 1
    await push(a, seq, Report(node_key("n2"), OrderKey(ts, "source-a", seq), {"cluster_id": "c1", "name": "node-2", "capacity": "8c16g"}))
    seq += 1
    await push(a, seq, Report(workload_key("w1"), OrderKey(ts, "source-a", seq), {"node_id": "n1", "name": "web", "kind": "deployment", "replicas": 2}))
    seq += 1
    await push(a, seq, Report(workload_key("w2"), OrderKey(ts, "source-a", seq), {"node_id": "n2", "name": "api", "kind": "deployment", "replicas": 1}))
    seq += 1
    await push(a, seq, Report(replica_key("r1"), OrderKey(ts, "source-a", seq), {"workload_id": "w1", "node_id": "n1", "name": "web-0", "phase": "running"}))
    seq += 1
    await push(a, seq, Report(replica_key("r2"), OrderKey(ts, "source-a", seq), {"workload_id": "w1", "node_id": "n1", "name": "web-1", "phase": "running"}))
    seq += 1
    await push(a, seq, Report(replica_key("r3"), OrderKey(ts, "source-a", seq), {"workload_id": "w2", "node_id": "n2", "name": "api-0", "phase": "running"}))

    # source-b reports cluster c2 and its subtree
    seq = 1
    await push(b, seq, Report(cluster_key("c2"), OrderKey(ts, "source-b", seq), {"name": "cluster-2"}))
    seq += 1
    await push(b, seq, Report(node_key("n3"), OrderKey(ts, "source-b", seq), {"cluster_id": "c2", "name": "node-3", "capacity": "2c4g"}))
    seq += 1
    await push(b, seq, Report(workload_key("w3"), OrderKey(ts, "source-b", seq), {"node_id": "n3", "name": "db", "kind": "statefulset", "replicas": 1}))
    seq += 1
    await push(b, seq, Report(replica_key("r4"), OrderKey(ts, "source-b", seq), {"workload_id": "w3", "node_id": "n3", "name": "db-0", "phase": "running"}))
