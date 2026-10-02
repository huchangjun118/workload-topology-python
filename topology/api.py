"""HTTP API for the workload topology service.

Endpoints expose hierarchical drill-down, scope filtering, view versions
and source health. Source control endpoints simulate disconnect /
reconnect / failure / recovery of the in-process data sources.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .aggregator import Aggregator
from .demo import build_demo_sources, seed_demo_reports
from .model import EntityStatus, EntityType
from .service import ForbiddenError, NotFoundError, Scope, TopologyService, ViewNotFoundError

#: Maximum time a read may take before the server gives up (504).
READ_TIMEOUT_S = 5.0


def _scope_from_request(request: Request) -> Scope:
    """Derive the caller scope from the ``clusters`` query parameter.

    ``?clusters=all`` (or no parameter) -> unrestricted.
    ``?clusters=c1,c2``                -> only those clusters.
    """
    raw = request.query_params.get("clusters")
    if raw is None or raw.strip() == "" or raw.strip() == "all":
        return Scope.all()
    clusters = frozenset(c.strip() for c in raw.split(",") if c.strip())
    return Scope(visible_clusters=clusters)


def _entity_json(entity: Any) -> dict[str, Any]:
    return {
        "id": entity.key.id,
        "type": entity.key.type.value,
        "status": entity.status.value,
        "contributors": sorted(entity.contributors),
        "payload": dict(entity.payload),
    }


def _layer_view_json(view: Any) -> dict[str, Any]:
    return {
        "layer": view.layer.value,
        "view_version": view.view_version,
        "entities": [_entity_json(e) for e in view.entities],
        "edges": [
            {"parent": {"type": e.parent.type.value, "id": e.parent.id},
             "child": {"type": e.child.type.value, "id": e.child.id}}
            for e in view.edges
        ],
    }


def create_app(aggregator: Aggregator | None = None) -> FastAPI:
    """Build the FastAPI app.

    When ``aggregator`` is given it is used directly (tests); otherwise a
    demo topology with in-memory sources is created on startup.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if aggregator is None:
            sources = build_demo_sources()
            agg = Aggregator(sources)
            await seed_demo_reports(sources)
        else:
            agg = aggregator
        await agg.start()
        app.state.aggregator = agg
        try:
            yield
        finally:
            await agg.stop()

    app = FastAPI(title="Workload Topology Service", lifespan=lifespan)

    def service(request: Request) -> TopologyService:
        return TopologyService(request.app.state.aggregator)

    # -- error handlers ----------------------------------------------------

    @app.exception_handler(NotFoundError)
    async def not_found_handler(request: Request, exc: NotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"error": "not_found", "detail": str(exc)})

    @app.exception_handler(ForbiddenError)
    async def forbidden_handler(request: Request, exc: ForbiddenError) -> JSONResponse:
        return JSONResponse(status_code=403, content={"error": "forbidden", "detail": str(exc)})

    @app.exception_handler(ViewNotFoundError)
    async def view_not_found_handler(request: Request, exc: ViewNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"error": "view_not_found", "detail": str(exc)})

    # -- read endpoints ----------------------------------------------------

    async def _read(coro):
        try:
            return await asyncio.wait_for(coro, timeout=READ_TIMEOUT_S)
        except asyncio.TimeoutError:
            return JSONResponse(
                status_code=504,
                content={"error": "read_timeout", "detail": "topology read timed out"},
            )

    @app.get("/")
    async def root() -> dict[str, str]:
        return {"service": "workload-topology", "docs": "/docs"}

    def _view_param(request: Request) -> int | None:
        raw = request.query_params.get("view")
        if raw is None or raw.strip() == "":
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    @app.get("/api/topology/clusters")
    async def list_clusters(request: Request):
        scope = _scope_from_request(request)
        view = _view_param(request)
        result = await _read(asyncio.to_thread(service(request).list_clusters, scope, view))
        return result if isinstance(result, JSONResponse) else _layer_view_json(result)

    @app.get("/api/topology/clusters/{cluster_id}/nodes")
    async def list_nodes(request: Request, cluster_id: str):
        scope = _scope_from_request(request)
        view = _view_param(request)
        result = await _read(asyncio.to_thread(service(request).list_nodes, scope, cluster_id, view))
        return result if isinstance(result, JSONResponse) else _layer_view_json(result)

    @app.get("/api/topology/nodes/{node_id}/workloads")
    async def list_workloads(request: Request, node_id: str):
        scope = _scope_from_request(request)
        view = _view_param(request)
        result = await _read(asyncio.to_thread(service(request).list_workloads, scope, node_id, view))
        return result if isinstance(result, JSONResponse) else _layer_view_json(result)

    @app.get("/api/topology/workloads/{workload_id}/replicas")
    async def list_replicas(request: Request, workload_id: str):
        scope = _scope_from_request(request)
        view = _view_param(request)
        result = await _read(asyncio.to_thread(service(request).list_replicas, scope, workload_id, view))
        return result if isinstance(result, JSONResponse) else _layer_view_json(result)

    @app.get("/api/topology/view")
    async def current_view(request: Request):
        snap = request.app.state.aggregator.current_snapshot()
        return {
            "view_version": snap.version,
            "source_health": [
                {"source_id": h.source_id, "state": h.state.value}
                for h in snap.source_health.values()
            ],
        }

    # -- source control (simulation) --------------------------------------

    @app.get("/api/sources")
    async def list_sources(request: Request):
        snap = request.app.state.aggregator.current_snapshot()
        return {
            "sources": [
                {"source_id": h.source_id, "state": h.state.value}
                for h in snap.source_health.values()
            ]
        }

    @app.post("/api/sources/{source_id}/disconnect")
    async def disconnect_source(request: Request, source_id: str):
        source = request.app.state.aggregator._sources.get(source_id)
        if source is None:
            return JSONResponse(status_code=404, content={"error": "not_found", "detail": f"source {source_id!r}"})
        await source.disconnect()
        return {"source_id": source_id, "connected": source.connected}

    @app.post("/api/sources/{source_id}/reconnect")
    async def reconnect_source(request: Request, source_id: str):
        source = request.app.state.aggregator._sources.get(source_id)
        if source is None:
            return JSONResponse(status_code=404, content={"error": "not_found", "detail": f"source {source_id!r}"})
        await source.reconnect()
        return {"source_id": source_id, "connected": source.connected}

    @app.post("/api/sources/{source_id}/fail")
    async def fail_source(request: Request, source_id: str):
        source = request.app.state.aggregator._sources.get(source_id)
        if source is None:
            return JSONResponse(status_code=404, content={"error": "not_found", "detail": f"source {source_id!r}"})
        await source.fail()
        return {"source_id": source_id, "failed": source.failed}

    @app.post("/api/sources/{source_id}/recover")
    async def recover_source(request: Request, source_id: str):
        source = request.app.state.aggregator._sources.get(source_id)
        if source is None:
            return JSONResponse(status_code=404, content={"error": "not_found", "detail": f"source {source_id!r}"})
        await source.recover()
        return {"source_id": source_id, "failed": source.failed}

    return app
