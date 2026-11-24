import types

import pytest

import config
import kube
import plan_store
import storage
from app import create_app
from routes import api
from routes.reports import safe_key

LT = {
    "metadata": {"namespace": "perf", "name": "sr-flow-1", "creationTimestamp": "2026-11-17T10:00:00Z"},
    "spec": {"workers": 3, "plan": {"file": "sr.yaml", "configMap": "sr-tests"}, "metrics": {"enabled": True, "port": 9464}},
    "status": {"phase": "Completed", "result": "Passed", "workersSucceeded": 3, "plan": "sr-email-status",
               "runDir": "s3://perf-results/runs/sr-flow-1-abcd1234",
               "reportPath": "s3://perf-results/runs/sr-flow-1-abcd1234/report/merged/index.html"},
}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(config, "WATCH_NAMESPACES", ["perf"])
    monkeypatch.setattr(config, "RESULTS_URL", "s3://perf-results/runs")
    monkeypatch.setattr(kube, "list_loadtests", lambda nss: [LT])
    monkeypatch.setattr(kube, "get_loadtest", lambda ns, name: LT)
    monkeypatch.setattr(kube, "jobs_for", lambda ns, name: [])
    monkeypatch.setattr(kube, "pods_for", lambda ns, name: [])
    created = {}
    monkeypatch.setattr(kube, "create_loadtest", lambda ns, body: created.setdefault("body", body) or body)
    deleted = {}
    monkeypatch.setattr(kube, "delete_loadtest", lambda ns, name: deleted.setdefault("name", name))
    monkeypatch.setattr(storage, "run_summary", lambda run_dir: {"run": {"users": 9}, "workers": [{"worker": 0}]})
    monkeypatch.setattr(config, "PLANS_URL", "s3://perf-results/test-plans")
    monkeypatch.setattr(plan_store, "file_exists", lambda suite, path: True)
    c = create_app().test_client()
    c.created, c.deleted = created, deleted
    return c


def test_list_shows_status_and_report_link(client):
    rows = client.get("/api/loadtests").get_json()
    assert rows[0]["name"] == "sr-flow-1" and rows[0]["phase"] == "Completed" and rows[0]["result"] == "Passed"
    assert rows[0]["report"] == "runs/sr-flow-1-abcd1234/report/merged/index.html"


def test_detail_and_unwatched_namespace(client):
    d = client.get("/api/loadtests/perf/sr-flow-1").get_json()
    assert d["spec"]["workers"] == 3 and d["pods"] == [] and d["jobs"] == []
    assert client.get("/api/loadtests/other/x").status_code == 404


def test_summary_from_s3(client):
    s = client.get("/api/loadtests/perf/sr-flow-1/summary").get_json()
    assert s["available"] and s["run"]["users"] == 9


def test_metrics_with_no_running_workers(client):
    m = client.get("/api/loadtests/perf/sr-flow-1/metrics").get_json()
    assert m["enabled"] and m["workers"] == [] and m["total"]["ok"] == 0


def test_create_builds_a_valid_loadtest(client):
    r = client.post("/api/loadtests", json={
        "plan": {"file": "plans/orders.yaml", "suite": "orders"},
        "workers": 4, "env": {"BASE_URL": "https://sit"}, "secretRefs": ["perf-secrets"], "memory": "2Gi"})
    assert r.status_code == 201
    body = client.created["body"]
    assert body["kind"] == "LoadTest" and body["metadata"]["name"].startswith("orders-")
    spec = body["spec"]
    assert spec["workers"] == 4 and spec["plan"] == {"file": "plans/orders.yaml", "s3": "s3://perf-results/test-plans/orders"}
    assert spec["results"]["s3"]["url"] == "s3://perf-results/runs" and spec["metrics"]["enabled"]
    assert spec["resources"]["limits"]["memory"] == "2Gi" and spec["env"] == {"BASE_URL": "https://sit"}
    assert spec["spread"] == "soft" and "reportResources" not in spec      # defaults


def test_create_with_spread_and_report_memory(client):
    r = client.post("/api/loadtests", json={"plan": {"file": "p.yaml", "suite": "s"}, "spread": "hard", "reportMemory": "6Gi"})
    assert r.status_code == 201
    spec = client.created["body"]["spec"]
    assert spec["spread"] == "hard" and spec["reportResources"]["limits"]["memory"] == "6Gi"
    bad = client.post("/api/loadtests", json={"plan": {"file": "p.yaml", "suite": "s"}, "spread": "everywhere"})
    assert bad.status_code == 400 and "spread" in bad.get_json()["error"]


@pytest.mark.parametrize("body,error", [
    ({"plan": {"suite": "s"}}, "plan.file"),
    ({"plan": {"file": "p.yaml"}}, "saved test plan suite"),
    ({"plan": {"file": "p.yaml", "configMap": "c"}}, "saved test plan suite"),       # not offered from the dashboard
    ({"plan": {"file": "p.yaml", "git": {"repo": "r"}}}, "saved test plan suite"),
    ({"plan": {"file": "p.yaml", "suite": "../x"}}, "suite name"),
    ({"plan": {"file": "p.yaml", "suite": "s"}, "workers": 0}, "between 1 and 200"),
    ({"plan": {"file": "p.yaml", "suite": "s"}, "name": "Bad_Name"}, "lower case"),
    ({"plan": {"file": "p.yaml", "suite": "s"}, "namespace": "kube-system"}, "not watched"),
])
def test_create_rejects_bad_input(client, body, error):
    r = client.post("/api/loadtests", json=body)
    assert r.status_code == 400 and error in r.get_json()["error"]


def test_delete(client):
    assert client.delete("/api/loadtests/perf/sr-flow-1").status_code == 200
    assert client.deleted["name"] == "sr-flow-1"


@pytest.mark.parametrize("path,ok", [
    ("runs/x/report/merged/index.html", True),
    ("runs/x/report/merged/js/stats.js", True),
    ("runs/../secret.txt", False),
    ("other-prefix/x.html", False),
    ("/runs/x.html", False),
    ("runs\\x.html", False),
])
def test_report_path_guard(monkeypatch, path, ok):
    monkeypatch.setattr(config, "RESULTS_URL", "s3://perf-results/runs")
    assert (safe_key(path) is not None) == ok


def test_unknown_api_path_is_404_not_the_spa(client):
    assert client.get("/api/nope").status_code == 404
