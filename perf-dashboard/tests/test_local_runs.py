"""Local runs end to end: the real perf distribution as processes (validate -> setup -> 2 workers -> report) against a
tiny HTTP server, through the dashboard's API - no Kubernetes, no S3."""
import json
import pathlib
import shutil
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import config
import local_runs
import storage
from app import create_app

REPO = pathlib.Path(__file__).resolve().parents[2]
PERF_HOME = REPO / "gatling-runner/target/perf-1.0-SNAPSHOT/perf-1.0-SNAPSHOT"
JAVA = shutil.which("java")
pytestmark = pytest.mark.skipif(not (JAVA and (PERF_HOME / "lib").is_dir()),
                                reason="needs java and the perf distribution (mvn -pl gatling-runner -am package)")

PLAN = """name: local-ping
baseUrl: ${BASE_URL}
apis:
  items: { request: GET /items, checks: [ { status: 200 } ] }
  ping: { request: "GET /ping?id=#{id}", checks: [ { status: 200 } ] }
setup:
  steps:
    - api: items
      extract: { ids: { jsonPath: "$.items[*]", all: true } }
  share:
    idQueue: { from: ids, as: id }
scenarios:
  - name: s
    feeders: [ idQueue ]
    steps: [ { api: ping } ]
    load: { profile: [ { atOnceUsers: 6 } ] }
sla:
  - global.failedRequests.percent < 1
"""


class _Api(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        body = json.dumps({"items": [1, 2, 3, 4, 5, 6]} if self.path.startswith("/items") else {"ok": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


@pytest.fixture
def api_server():
    s = ThreadingHTTPServer(("127.0.0.1", 0), _Api)
    threading.Thread(target=s.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{s.server_address[1]}"
    s.shutdown()


@pytest.fixture
def local(tmp_path, monkeypatch):
    for k, v in {"EXECUTOR": "local", "STORAGE": "folder", "STORAGE_DIR": str(tmp_path),
                 "LOCAL_STATE_DIR": str(tmp_path / ".dashboard"), "WATCH_NAMESPACES": ["local"],
                 "PERF_HOME": str(PERF_HOME), "PERF_JAVA": JAVA, "RESULTS_URL": "s3://perf-results/runs",
                 "PLANS_URL": "s3://perf-results/test-plans", "LOCAL_MAX_WORKERS": 4}.items():
        monkeypatch.setattr(config, k, v)
    monkeypatch.setattr(storage, "_client", None)
    monkeypatch.setattr(storage, "_cache", {})
    monkeypatch.setattr(local_runs, "_runs", {})
    monkeypatch.setattr(local_runs, "_loaded", False)
    suite = tmp_path / "perf-results/test-plans/local-demo/plans"
    suite.mkdir(parents=True)
    (suite / "ping.yaml").write_text(PLAN)
    (suite / "bad.yaml").write_text(PLAN.replace("api: ping", "api: nope"))
    return create_app().test_client()


def _start(c, name, file, server, workers=2):
    return c.post("/api/loadtests", json={"namespace": "local", "name": name, "workers": workers, "startDelaySeconds": 3,
                                          "plan": {"suite": "local-demo", "file": f"plans/{file}"}, "env": {"BASE_URL": server}})


def _wait(c, name, timeout=240):
    end = time.time() + timeout
    while time.time() < end:
        d = c.get(f"/api/loadtests/local/{name}").get_json()
        if d["phase"] in ("Completed", "Failed"):
            return d
        time.sleep(1)
    pytest.fail(f"{name} did not finish: {d['phase']} {d.get('message')}")


def test_a_local_run_goes_through_every_phase(local, api_server, tmp_path):
    r = _start(local, "ping-1", "ping.yaml", api_server)
    assert r.status_code == 201, r.get_json()
    d = _wait(local, "ping-1")
    assert d["phase"] == "Completed" and d["result"] == "Passed", d
    roles = [p["role"] for p in d["pods"]]
    assert sorted(roles) == ["report", "setup", "validate", "workers", "workers"], roles
    assert all(p["phase"] == "Succeeded" for p in d["pods"])
    assert {j["role"] for j in d["jobs"]} == {"validate", "setup", "workers", "report"}

    # same run folder layout as in MinIO: test copy, shared lists, worker logs + numbers, merged report + summary
    folder = tmp_path / "perf-results/runs" / d["runDir"].rsplit("/", 1)[-1]
    for f in ["tests/plans/ping.yaml", "shared.json", "logs/worker-0.log", "logs/worker-1.log", "logs/worker-0.json",
              "report/summary.json", "report/merged/index.html"]:
        assert (folder / f).is_file(), f
    s = local.get("/api/loadtests/local/ping-1/summary").get_json()
    assert s["available"] and s["run"]["total"]["count"] == 6 + 0 and s["run"]["total"]["ko"] == 0   # 6 pings (setup not merged)
    assert sorted(w["total"]["count"] for w in s["workers"]) == [3, 3]                                # split 3 + 3
    assert local.get("/reports/" + d["report"]).status_code == 200
    worker = next(p for p in d["pods"] if p["role"] == "workers")
    log = local.get(f"/api/pods/local/{worker['name']}/logs").get_json()["log"]
    assert "Worker 1/2" in log
    assert local.get("/api/runs").get_json()[0]["summary"]["slaPassed"] is True

    # delete with results: processes gone, run folder gone
    assert local.delete("/api/loadtests/local/ping-1?results=true").status_code == 200
    assert not folder.exists() and local.get("/api/loadtests/local/ping-1").status_code == 404


def test_an_invalid_plan_fails_in_validating_with_the_reason(local, api_server):
    assert _start(local, "bad-1", "bad.yaml", api_server, workers=1).status_code == 201
    d = _wait(local, "bad-1")
    assert d["phase"] == "Failed" and "plan is invalid" in d["message"] and "nope" in d["message"], d["message"]
    assert [p["role"] for p in d["pods"]] == ["validate"]


def test_limits_and_names(local, api_server):
    r = _start(local, "big-1", "ping.yaml", api_server, workers=5)
    assert r.status_code == 400 and "at most 4 worker processes" in r.get_json()["error"]
    assert _start(local, "dup-1", "bad.yaml", api_server, workers=1).status_code == 201
    assert _start(local, "dup-1", "bad.yaml", api_server, workers=1).status_code == 409
    _wait(local, "dup-1")


def test_runs_survive_a_restart_as_records_active_ones_are_marked_failed(local, tmp_path):
    state = tmp_path / ".dashboard/runs"
    state.mkdir(parents=True)
    for name, phase in [("old-done", "Completed"), ("old-running", "Running")]:
        (state / f"local__{name}.json").write_text(json.dumps({
            "metadata": {"name": name, "namespace": "local", "creationTimestamp": "2026-11-25T10:00:00Z"},
            "spec": {"workers": 1, "plan": {"file": "x"}}, "status": {"phase": phase, "runDir": f"s3://perf-results/runs/{name}-1"}}))
    rows = {r["name"]: r for r in local.get("/api/loadtests").get_json()}
    assert rows["old-done"]["phase"] == "Completed"
    assert rows["old-running"]["phase"] == "Failed" and "dashboard stopped" in rows["old-running"]["message"]
