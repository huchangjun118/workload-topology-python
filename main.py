"""HTTP entrypoint for the workload topology browsing service."""

from __future__ import annotations

from typing import List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request

from topology import (
    ForbiddenError,
    Level,
    NotFoundError,
    Scope,
    TopologyService,
    TopologyStore,
)
from topology.errors import CancelledError
from topology.errors import TimeoutError as TopologyTimeoutError

store = TopologyStore()
service = TopologyService(store)
app = FastAPI(title="workload-topology")


def _scope(
    clusters: Optional[List[str]],
    nodes: Optional[List[str]],
    workloads: Optional[List[str]],
) -> Scope:
    return Scope(
        clusters=frozenset(clusters) if clusters else None,
        nodes=frozenset(nodes) if nodes else None,
        workloads=frozenset(workloads) if workloads else None,
    )


@app.get("/topology/{level}")
def get_level(
    level: Level,
    request: Request,
    parent_id: Optional[str] = None,
    cluster: Optional[List[str]] = Query(default=None),
    node: Optional[List[str]] = Query(default=None),
    workload: Optional[List[str]] = Query(default=None),
    timeout: Optional[float] = Query(default=None),
):
    try:
        view = service.get_level(
            level,
            _scope(cluster, node, workload),
            parent_id,
            timeout=timeout,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ForbiddenError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except (CancelledError, TopologyTimeoutError) as exc:
        raise HTTPException(status_code=408, detail=str(exc)) from exc
    return {
        "level": view.level.value,
        "view_version": view.view_version,
        "entities": [
            {
                "kind": e.kind.value,
                "id": e.id,
                "parent_id": e.parent_id,
                "health": e.health.value,
                "freshness": e.freshness.value,
                "version": e.version,
                "sources": list(e.sources),
                "labels": dict(e.labels),
            }
            for e in view.entities
        ],
        "edges": [
            {
                "parent_id": e.parent_id,
                "child_id": e.child_id,
                "freshness": e.freshness.value,
            }
            for e in view.edges
        ],
        "sources": [
            {"source_id": s.source_id, "connected": s.connected, "last_seq": s.last_seq}
            for s in view.sources
        ],
    }


@app.get("/")
def read_root():
    return {"service": "workload-topology", "view_version": store.view_version()}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
