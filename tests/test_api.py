"""HTTP surface: level endpoints, scope params, 404 vs 403 mapping."""

from __future__ import annotations

from fastapi.testclient import TestClient

from main import app, store
from topology import DataSource, Level
from topology import Change

from .conftest import node_change, replica_change, workload_change


def _client():
    src = DataSource("api", store)
    src.connect()
    src.emit([node_change("n1", "c1", 1)])
    src.emit([workload_change("w1", "n1", 1)])
    src.emit([replica_change("r1", "w1", 1)])
    return TestClient(app)


def test_cluster_endpoint():
    client = _client()
    resp = client.get("/topology/cluster")
    assert resp.status_code == 200
    body = resp.json()
    assert body["level"] == "cluster"
    assert [e["id"] for e in body["entities"]] == ["c1"]
    assert body["view_version"] >= 1
    assert body["sources"][0]["source_id"] == "api"


def test_drilldown_endpoints():
    client = _client()
    resp = client.get("/topology/node", params={"parent_id": "c1"})
    assert [e["id"] for e in resp.json()["entities"]] == ["n1"]
    resp = client.get("/topology/workload", params={"parent_id": "n1"})
    assert [e["id"] for e in resp.json()["entities"]] == ["w1"]
    resp = client.get("/topology/replica", params={"parent_id": "w1"})
    assert [e["id"] for e in resp.json()["entities"]] == ["r1"]


def test_scope_query_params():
    client = _client()
    resp = client.get("/topology/node", params={"cluster": "c-other"})
    assert resp.json()["entities"] == []


def test_404_and_403_mapping():
    client = _client()
    resp = client.get("/topology/node", params={"parent_id": "nope"})
    assert resp.status_code == 404
    resp = client.get("/topology/node", params={"parent_id": "c1", "cluster": "c2"})
    assert resp.status_code == 403
