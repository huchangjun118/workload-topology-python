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

`stale` means *may be out of date*, not *gone*: stale entities are still
returned by every drill-down layer with their edges, and scope filtering
(`403` vs `404`) applies to them exactly as to `active` entities. The
status is computed per published snapshot, so a response never shows an
entity under one status in one layer and another status in another layer
when both reads pin the same `view`.

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
* The merge itself runs in one synchronous, non-interruptible step; the
  snapshot reference is swapped only after the whole snapshot is built, so
  neither cancellation nor timeout can publish or leave behind a
  half-built graph.

## Concurrency and failure-timing semantics

The acceptance suite (`tests/test_concurrency.py`) pins down the behavior
under sustained concurrent load and failure timing, with a topology that is
deliberately much larger than the basic tests (multiple sources, six
clusters with six nodes each and eight workloads per node) and with many
more readers than writers:

* **Continuous writes during reads.** While a writer keeps mutating the
  graph, every concurrent drill-down response is attributed to exactly one
  `view_version`: every entity and edge in the response is re-verified
  against the immutable snapshot of that version. A response can never mix
  two versions or contain an edge to an entity that is not present in the
  same version.
* **No rollback under read-heavy pressure.** Versions are monotonic; newer
  reads never observe an older graph than an earlier read, and the same
  pinned version returns byte-identical entities no matter how many writes
  happen in between. The pressure scenario is repeated for several rounds
  and must pass every time.
* **Read cancellation.** Cancelling a read that is parked mid-flight
  propagates `CancelledError` to the caller, leaves no in-flight read task
  behind, and subsequent writes still publish complete, internally
  consistent snapshots. Cancellation of a waiter parked on a future view
  version is also clean and does not poison later waiters or publications.
* **Read timeout.** A read wrapped in `asyncio.wait_for` that exceeds its
  deadline raises `TimeoutError`; the wrapped read is fully cancelled and
  the next reads work normally against the latest version. The HTTP layer
  translates the same condition into `504 read_timeout`.
* **Single-source failure -> stale subtree.** When one source fails, the
  subtrees it covers *solely* are marked `stale` (whole cluster subtree in
  the scaled scenario), while entities corroborated by another alive source
  stay `active` and the rest of the graph stays fully readable. Stale data
  is still returned on every layer, edges stay intact, and every response
  still carries its consistent `view_version`.
* **Automatic stale clearing on recovery.** When the source recovers it
  flushes its buffered outbox and resumes heartbeats; once an alive
  snapshot is published all `stale` markers it solely caused clear by
  themselves — no manual reset, no stale residue.

Synchronization in these tests is event-driven (waiting on actual
publications) rather than based on fixed sleeps, so repeated runs on
loaded or slow machines give the same result.

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

### Prerequisites

* [uv](https://docs.astral.sh/uv/) (a recent release). No system Python
  needs to be preinstalled: the project pins `requires-python = ">=3.14.4"`
  and `uv` downloads a compatible interpreter automatically on first use.
* Outbound access to the Python package index configured in
  `pyproject.toml` is required only during dependency installation; the
  service itself and all tests run fully offline afterwards (no external
  cluster, credential or network).

### Clean-environment install and verification

On a brand-new machine, with no pre-existing virtual environment or
packages, the following is the complete procedure:

```bash
# 1. create .venv and install exactly what uv.lock records
#    (works repeatedly; a second run is a no-op)
uv sync --frozen

# 2. run the service (demo topology with two in-memory sources)
uv run python main.py

# 3. run the tests
uv run pytest
```

`uv sync --frozen` refuses to run if `pyproject.toml` and `uv.lock` ever
diverge, so a stale lockfile fails loudly instead of silently installing
different packages. `uv sync` (without `--frozen`) may be used while
developing; it updates `uv.lock` in place. Recreating the environment from
scratch (`rm -rf .venv && uv sync --frozen`) and reinstalling on top of an
existing environment produce the same package set.

### Test runtime requirements

* The test suite is asynchronous. The required async runner
  (`pytest-asyncio`) is a **declared, locked dependency**; do not rely on a
  copy that happens to exist in a developer's global environment.
* `asyncio_mode = "auto"` means every `async def test_*` coroutine is
  executed as a real asyncio test; none needs an explicit marker.
* A conftest hook converts any skipped test into a failure, so a missing
  async plugin (or any other silent skip) fails the run instead of showing
  a green result that executed nothing.
* Tests never assume a particular machine speed: they synchronize on
  concrete view versions / source states via
  `Aggregator.wait_for_version()`, `Aggregator.wait_for_source_state()` and
  `Aggregator.wait_until()`, or use a polling loop bounded by a generous
  deadline. Timeouts in tests are safety nets, not timing assumptions.

The interactive API docs are available at `/docs` when the service runs.
