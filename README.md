# Workload Topology Service

A self-contained workload topology browsing service. Multiple in-process
**data sources** push node / workload / replica changes through memory
pipes; an **aggregator** merges them into a single consistent topology
graph and serves hierarchical drill-down with scope filtering.

No external cluster, credential or network is involved.

## Hierarchy

```
cluster -> node -> workload -> replica
```

Each drill-down layer returns only the entities and the direct
parent->child edges belonging to that layer. Cross-layer edges are never
emitted and orphan entities (a child whose parent is missing or out of
scope) never appear in a response.

## Architecture

| Module | Responsibility |
| --- | --- |
| `topology/model.py` | Entity / edge / order-key types, status enums |
| `topology/sources.py` | In-memory data sources: pipes, heartbeats, disconnect / reconnect / fail / recover |
| `topology/aggregator.py` | Per-source state, deterministic merge, immutable snapshots, view versions |
| `topology/service.py` | Drill-down queries, scope filtering, 403 vs 404 distinction |
| `topology/api.py` | FastAPI HTTP endpoints and source-control simulation |
| `topology/demo.py` | Demo sources and sample topology |

## View consistency semantics

* **Immutable snapshots.** The aggregator keeps per-source latest reports
  and merges them into a new immutable *snapshot* on every change batch.
  Reads always reference a snapshot object; while a read is in flight the
  aggregator may publish newer snapshots, but the in-flight read keeps
  pointing at the old one. A response is therefore internally consistent
  and never mixes two versions.
* **View versions.** Every snapshot has a monotonically increasing
  `view_version`. Every response carries the version it was served from.
  Clients can pin a version with `?view=N` so that several drill-down
  calls (clusters -> nodes -> workloads -> replicas) all read the same
  consistent view. A pinned version that has aged out of the history
  window is answered with `409 view_not_found`.
* **Atomic publication.** Snapshots are built in one synchronous,
  non-interruptible step and published by swapping a single reference.
  Task cancellation can therefore never leave the aggregation in an
  intermediate state, and subsequent reads never see a half-built result.

## Merge rules (order-independent)

Every report carries a totally ordered key
`(timestamp_ns, source_id, seq)`. For each entity key:

* the report with the greatest order key wins;
* a winning tombstone (`deleted=true`) removes the entity, and an older
  report arriving afterwards can never resurrect it;
* the result depends only on the set of reports, **not on arrival order**.

This makes the merge convergent under out-of-order delivery and safe for
duplicate folding: re-delivering the same event (same order key) is a
no-op and never bumps the view version.

## Status markers

Entity status (`entity.status`):

| Status | Meaning |
| --- | --- |
| `active` | At least one source contributing the entity is alive. |
| `stale` | All contributing sources have failed / disconnected. The entity is still readable but its data may be out of date. |

Source health (`GET /api/sources`):

| State | Meaning |
| --- | --- |
| `alive` | The source is connected and heartbeating (or sending reports). |
| `dead` | The source has been silent longer than its `fail_timeout`. |

When a source fails, the subtree it solely covers is marked `stale`;
entities corroborated by other alive sources stay `active`, and the other
sources' data remains fully readable. When the source recovers it flushes
its buffered outbox and resumes heartbeats, so the `stale` state clears
automatically without any manual intervention.

## Delivery semantics

* **Disconnect buffering.** While a source is disconnected, reports are
  buffered in its outbox and re-delivered in order on reconnect. The view
  never rolls back to an older version.
* **Out-of-order arrival.** Reports are applied by order key, so a late
  older report cannot overwrite a newer one.
* **Duplicate folding.** Re-delivered or duplicated events (same order
  key) are folded and produce no new version.

## Scope filtering

Callers present a visibility scope via the `clusters` query parameter:

* `?clusters=all` (or omitted) -> unrestricted;
* `?clusters=c1,c2` -> only those clusters and their subtrees.

Scope is enforced at every layer and on every incident edge. The two
authorization outcomes are deliberately distinguished:

* entity exists but is outside the caller's scope -> `403 forbidden`;
* entity does not exist in the merged graph -> `404 not_found`.

## Cancellation and timeout

* Reads are served from immutable snapshots, so cancelling a read (client
  disconnect) cannot corrupt the aggregation.
* Server-side reads are bounded by `READ_TIMEOUT_S`; a timeout returns
  `504 read_timeout` and leaves the aggregator state intact.

## API

| Method | Path | Description |
| --- | --- | --- |
| GET | `/api/topology/clusters` | List clusters (layer 0) |
| GET | `/api/topology/clusters/{cluster_id}/nodes` | List nodes of a cluster (layer 1) |
| GET | `/api/topology/nodes/{node_id}/workloads` | List workloads of a node (layer 2) |
| GET | `/api/topology/workloads/{workload_id}/replicas` | List replicas of a workload (layer 3) |
| GET | `/api/topology/view` | Current view version and source health |
| GET | `/api/sources` | Source health |
| POST | `/api/sources/{id}/disconnect` | Simulate source disconnect |
| POST | `/api/sources/{id}/reconnect` | Reconnect a disconnected source |
| POST | `/api/sources/{id}/fail` | Hard-fail a source |
| POST | `/api/sources/{id}/recover` | Recover a failed source |

All layer endpoints accept `clusters` (scope) and `view` (pinned version).

## Running

```bash
# install dependencies
uv sync

# run the service (demo topology with two in-memory sources)
uv run python main.py

# run the tests
uv run pytest
```

The interactive API docs are available at `/docs` when the service runs.
