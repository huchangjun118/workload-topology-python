"""HTTP API tests using FastAPI's TestClient."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from topology.aggregator import Aggregator
from topology.api import create_app
from topology.service import Scope

from .conftest import full_topology, push_all


def _make_client(sources):
    agg = Aggregator(sources)
    app = create_app(agg)
    return TestClient(app), agg


async def _seed(source):
    await push_all(source, full_topology("s1"))


def test_drill_down_endpoints(make_source):
    source = make_source("s1")
    asyncio.run(_seed(source))
    client, agg = _make_client([source])
    with client:
        r = client.get("/api/topology/clusters")
        assert r.status_code == 200
        body = r.json()
        assert [e["id"] for e in body["entities"]] == ["c1"]
        assert body["view_version"] == agg.version

        r = client.get("/api/topology/clusters/c1/nodes")
        assert r.status_code == 200
        assert [e["id"] for e in r.json()["entities"]] == ["n1"]

        r = client.get("/api/topology/nodes/n1/workloads")
        assert r.status_code == 200
        assert [e["id"] for e in r.json()["entities"]] == ["w1"]

        r = client.get("/api/topology/workloads/w1/replicas")
        assert r.status_code == 200
        assert [e["id"] for e in r.json()["entities"]] == ["r1", "r2"]


def test_scope_filtering_via_query_param(make_source):
    source = make_source("s1")
    asyncio.run(_seed(source))
    client, _ = _make_client([source])
    with client:
        # Scope c2 -> c1 exists but is forbidden.
        r = client.get("/api/topology/clusters/c1/nodes?clusters=c2")
        assert r.status_code == 403
        # Missing entity -> 404.
        r = client.get("/api/topology/clusters/cX/nodes")
        assert r.status_code == 404
        # Unrestricted.
        r = client.get("/api/topology/clusters/c1/nodes?clusters=all")
        assert r.status_code == 200


def test_source_control_endpoints(make_source):
    source = make_source("s1")
    asyncio.run(_seed(source))
    client, agg = _make_client([source])
    with client:
        r = client.get("/api/sources")
        assert r.status_code == 200
        assert r.json()["sources"][0]["state"] == "alive"

        r = client.post("/api/sources/s1/fail")
        assert r.status_code == 200
        assert r.json()["failed"] is True

        # Wait for the fail timeout, then the source is reported dead.
        import time
        time.sleep(0.6)
        r = client.get("/api/sources")
        assert r.json()["sources"][0]["state"] == "dead"

        # Recover.
        r = client.post("/api/sources/s1/recover")
        assert r.status_code == 200
        time.sleep(0.3)
        r = client.get("/api/sources")
        assert r.json()["sources"][0]["state"] == "alive"


def test_pinned_view_via_query_param(make_source):
    source = make_source("s1")
    asyncio.run(_seed(source))
    client, agg = _make_client([source])
    with client:
        v = client.get("/api/topology/view").json()["view_version"]
        r = client.get(f"/api/topology/clusters?view={v}")
        assert r.status_code == 200
        assert r.json()["view_version"] == v

        # A view version that is too old (beyond history) -> 409.
        r = client.get("/api/topology/clusters?view=999999")
        assert r.status_code == 409
