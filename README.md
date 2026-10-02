# Workload Topology Browsing Service

Aggregates node / workload / replica change streams from multiple
in-process data sources (in-memory pipes, no external cluster, credentials
or network) into one unified topology, and serves scoped, level-by-level
drill-down reads over HTTP.

## Layout

| File | Role |
| --- | --- |
| `topology/models.py` | Immutable domain types: `Node`, `Workload`, `Replica`, `Scope`, `Change`, view DTOs |
| `topology/store.py` | `TopologyStore` — versioned multi-source aggregation core |
| `topology/source.py` | `DataSource` — in-memory pipe with disconnect buffering / reconnect replay |
| `topology/service.py` | `TopologyService` — scoped drill-down reads, cancellation/timeout |
| `topology/errors.py` | `NotFoundError` vs `ForbiddenError`, `CancelledError`, `TimeoutError` |
| `main.py` | FastAPI entrypoint (`GET /topology/{cluster|node|workload|replica}`) |
| `tests/` | Aggregation, drill-down, degradation, concurrency and API tests |

## Aggregation & merge semantics

Each source pushes batches of `Change`s tagged with a per-source,
monotonically increasing sequence number and a per-entity version.

* **Per (entity, source):** last-write-wins by entity version. A change
  whose version is not greater than the stored one is dropped, so
  out-of-order delivery never regresses the view.
* **Per (source, batch):** a batch whose sequence number is not greater
  than the last applied one is folded away, so duplicated replays are
  idempotent.
* **Across sources:** the visible record of an entity is the live record
  with the highest version; ties are broken by lexicographic source id.
  The merge is a pure function of the per-(entity, source) maxima, hence
  **independent of arrival order**.
* **Deletes:** an entity disappears only after *every* source that
  reported it has deleted it.

## View consistency semantics

* The store is guarded by a single lock; every applied batch bumps a
  global **view version**.
* Every read takes **one atomic snapshot** and the whole response —
  entities, edges, source statuses — is computed from it. A response is
  therefore always attributable to exactly one `view_version`, and
  changes arriving while a read is in progress never leak into it.
* **Cancellation / timeout** are checked before and after the snapshot
  and before the response is assembled. A cancelled or timed-out read
  raises (`CancelledError` / `TimeoutError`, mapped to HTTP 408) and
  publishes nothing; the store is never mutated by reads, so no
  intermediate state is observable and subsequent reads are unaffected.

## Drill-down levels

`cluster → node → workload → replica`. Each level returns only its own
entities plus containment edges to the immediate parent:

* entities whose parent is missing (not reported or filtered out) are
  suppressed — no orphans, no dangling edges;
* every edge's child is in the same response's entity set, and when a
  `parent_id` is given every edge starts at that parent.

## Scope filtering and 404 vs 403

`Scope` restricts visibility along cluster / node / workload dimensions
(`None` = unrestricted). Scoped-out entities and their associated edges
never appear in any level's response. When drilling down by `parent_id`:

* parent **absent** from the aggregated view → `NotFoundError` (HTTP 404);
* parent **present but outside the caller's scope** → `ForbiddenError`
  (HTTP 403).

## Freshness / staleness markers

Each `EntityView` and `EdgeView` carries a `freshness` field:

* `live` — at least one source currently reporting the entity is connected;
* `stale` — **every** source currently reporting the entity is
  disconnected (single-source failure degrades only that source's
  subtree; all other data stays readable and `live`).

Staleness is derived from connectivity at read time, so it **clears
automatically** when a contributing source reconnects. While
disconnected, a source buffers its changes and replays them in order on
reconnect (catch-up), so no update is lost. `LevelView.sources` exposes
per-source `connected` / `last_seq` so callers can distinguish degraded
sources directly.

## HTTP API

```
GET /topology/cluster
GET /topology/node?parent_id=<cluster>
GET /topology/workload?parent_id=<node>
GET /topology/replica?parent_id=<workload>
```

Query parameters: `cluster`, `node`, `workload` (repeatable scope
filters), `timeout` (seconds). Errors: 404 not found, 403 out of scope,
408 cancelled/timed out. Every 200 response includes `view_version`,
`entities[]` (with `freshness`, `sources`, `version`), `edges[]` and
`sources[]`.

## Development

```
uv sync
uv run pytest          # 34 tests
uv run python main.py  # serve on 127.0.0.1 (ephemeral port)
```
