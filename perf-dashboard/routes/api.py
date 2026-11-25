"""JSON API for the React app."""
import re
from datetime import datetime, timezone

from flask import Blueprint, abort, jsonify, request

import config
import live
import plan_store
import runs
import storage
from runs import RunError

try:  # the kubernetes client is not needed (or installed) when runs are local
    from kubernetes.client.exceptions import ApiException
except ImportError:  # pragma: no cover
    ApiException = None

bp = Blueprint("api", __name__, url_prefix="/api")

_NAME = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
TERMINAL = {"Completed", "Failed"}


def _watched(ns):
    if ns not in config.WATCH_NAMESPACES:
        abort(404, description=f"namespace {ns} is not watched by this dashboard")


if ApiException is not None:
    @bp.errorhandler(ApiException)
    def _k8s_error(e):
        return jsonify(error=e.reason, status=e.status), e.status or 500


@bp.errorhandler(RunError)
def _run_error(e):
    return jsonify(error=str(e)), e.status


def _row(lt):
    meta, spec, st = lt.get("metadata", {}), lt.get("spec", {}), lt.get("status") or {}
    plan = spec.get("plan") or {}
    return {
        "namespace": meta.get("namespace"),
        "name": meta.get("name"),
        "created": meta.get("creationTimestamp"),
        "plan": st.get("plan") or plan.get("file"),
        "planFile": plan.get("file"),
        "workers": spec.get("workers", 1),
        "phase": st.get("phase") or "Pending",
        "message": st.get("message"),
        "result": st.get("result"),
        "workersSucceeded": st.get("workersSucceeded", 0),
        "workersFailed": st.get("workersFailed", 0),
        "startedAt": st.get("startedAt"),
        "startAt": st.get("startAt"),
        "finishedAt": st.get("finishedAt"),
        "runDir": st.get("runDir"),
        "reportPath": st.get("reportPath"),
        "metricsEnabled": bool((spec.get("metrics") or {}).get("enabled")),
    }


def _report_key(row):
    """s3://bucket/runs/x/report/merged/index.html -> runs/x/report/merged/index.html (only for our results bucket)."""
    path = row.get("reportPath") or ""
    bucket, _ = config.results_bucket_and_prefix()
    if path.startswith(f"s3://{bucket}/"):
        return path[len(f"s3://{bucket}/"):]
    return None


@bp.get("/health")
def health():
    return {"status": "ok"}


@bp.get("/config")
def get_config():
    return {
        "namespaces": config.WATCH_NAMESPACES,
        "resultsUrl": config.RESULTS_URL,
        "plansUrl": config.PLANS_URL,
        "executor": config.EXECUTOR,
        "storage": config.STORAGE,
        "localMaxWorkers": config.LOCAL_MAX_WORKERS,
        "defaults": {"image": config.DEFAULT_IMAGE, "s3Secret": config.DEFAULT_S3_SECRET, "s3Endpoint": config.DEFAULT_RUN_S3_ENDPOINT},
    }


@bp.get("/loadtests")
def list_loadtests():
    rows = [_row(lt) for lt in runs.backend().list_loadtests(config.WATCH_NAMESPACES)]
    for r in rows:
        r["report"] = _report_key(r)
    rows.sort(key=lambda r: r["created"] or "", reverse=True)
    return jsonify(rows)


@bp.get("/loadtests/<ns>/<name>")
def get_loadtest(ns, name):
    _watched(ns)
    lt = runs.backend().get_loadtest(ns, name)
    row = _row(lt)
    row["report"] = _report_key(row)
    row["spec"] = lt.get("spec", {})
    row["jobs"] = sorted(runs.backend().jobs(ns, name), key=lambda j: j["start"] or "")
    row["pods"] = sorted(runs.backend().pods(ns, name),
                         key=lambda p: (p["role"] != "workers", p["worker"] if p["worker"] is not None else 0, p["name"]))
    return jsonify(row)


@bp.get("/loadtests/<ns>/<name>/metrics")
def get_metrics(ns, name):
    _watched(ns)
    lt = runs.backend().get_loadtest(ns, name)
    metrics = (lt.get("spec") or {}).get("metrics") or {}
    if not metrics.get("enabled"):
        return {"enabled": False, "workers": [], "unreachable": [], "total": None}
    pods = runs.backend().pods(ns, name)
    return {"enabled": True, **live.scrape(pods, metrics.get("port", 9464), run_key=f"{ns}/{name}")}


@bp.get("/loadtests/<ns>/<name>/summary")
def get_summary(ns, name):
    _watched(ns)
    run_dir = ((runs.backend().get_loadtest(ns, name).get("status") or {}).get("runDir")) or ""
    if not run_dir.startswith("s3://"):
        return {"available": False, "reason": "results are not in S3 (pvc results are not readable by the dashboard)"}
    return {"available": True, **storage.run_summary(run_dir)}


@bp.get("/pods/<ns>/<pod>/logs")
def get_logs(ns, pod):
    _watched(ns)
    tail = min(max(int(request.args.get("tail", 500)), 10), 5000)
    return {"pod": pod, "log": runs.backend().pod_logs(ns, pod, tail)}


def build_loadtest(body):
    """Validates a New-run form and turns it into a LoadTest resource. Raises ValueError with a readable message."""
    ns = body.get("namespace") or config.WATCH_NAMESPACES[0]
    if ns not in config.WATCH_NAMESPACES:
        raise ValueError(f"namespace {ns} is not watched by this dashboard")
    plan = body.get("plan") or {}
    file = (plan.get("file") or "").strip()
    if not file:
        raise ValueError("plan.file is required")
    # runs from the dashboard use saved test plans (S3); Git / ConfigMap sources are for perf submit, CI and GitOps
    suite = (plan.get("suite") or "").strip()
    if not suite:
        raise ValueError("choose a saved test plan suite (plan.suite)")
    try:
        if not plan_store.file_exists(suite, file):
            raise ValueError(f"{file} is not in the saved suite {suite}")
    except plan_store.PlanError as e:
        raise ValueError(str(e))
    try:
        workers = int(body.get("workers", 1))
    except (TypeError, ValueError):
        raise ValueError("workers must be a number")
    if workers < 1 or workers > 200:
        raise ValueError("workers must be between 1 and 200")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    base = re.sub(r"[^a-z0-9-]", "-", file.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()).strip("-")[:40] or "run"
    name = (body.get("name") or f"{base}-{stamp}").strip().lower()
    if not _NAME.match(name) or len(name) > 60:
        raise ValueError("name must be lower case letters, digits and '-', at most 60 characters")

    spec_plan = {"file": file, "s3": plan_store.suite_url(suite)}   # the validate pod copies it into the run folder
    spec = {
        "image": (body.get("image") or config.DEFAULT_IMAGE).strip(),
        "plan": spec_plan,
        "workers": workers,
        "results": {"s3": {"url": config.RESULTS_URL, "credentialsSecret": config.DEFAULT_S3_SECRET,
                           "endpoint": config.DEFAULT_RUN_S3_ENDPOINT}},
        "metrics": {"enabled": bool(body.get("metrics", True))},
    }
    env = {k.strip(): str(v) for k, v in (body.get("env") or {}).items() if k and k.strip()}
    if env:
        spec["env"] = env
    secrets = [s.strip() for s in (body.get("secretRefs") or []) if s and s.strip()]
    if secrets:
        spec["secretRefs"] = secrets
    scenarios = [s.strip() for s in (body.get("scenarios") or []) if s and s.strip()]
    if scenarios:
        spec["scenarios"] = scenarios
    res = {k: str(body[k]).strip() for k in ("cpu", "memory") if body.get(k)}
    if res:
        spec["resources"] = {"requests": dict(res), "limits": dict(res)}
    spread = (body.get("spread") or "soft").strip()
    if spread not in ("soft", "hard", "none"):
        raise ValueError("spread must be soft, hard or none")
    spec["spread"] = spread
    if body.get("reportMemory"):
        mem = str(body["reportMemory"]).strip()
        spec["reportResources"] = {"requests": {"memory": mem}, "limits": {"memory": mem}}
    if body.get("startDelaySeconds"):
        spec["startDelaySeconds"] = max(5, int(body["startDelaySeconds"]))
    return ns, {"apiVersion": f"{runs.GROUP}/{runs.VERSION}", "kind": "LoadTest",
                "metadata": {"name": name, "namespace": ns}, "spec": spec}


@bp.post("/loadtests")
def create_loadtest():
    try:
        ns, body = build_loadtest(request.get_json(force=True) or {})
    except ValueError as e:
        return jsonify(error=str(e)), 400
    created = runs.backend().create_loadtest(ns, body)
    return jsonify(_row(created)), 201


def _results_folder(run_dir):
    """Run folder name when run_dir is under our results prefix (only those may be deleted from here)."""
    base = storage.run_folder_url("")
    if run_dir and run_dir.startswith(base):
        folder = run_dir[len(base):].strip("/")
        if _NAME.match(folder):
            return folder
    return None


@bp.delete("/loadtests/<ns>/<name>")
def delete_loadtest(ns, name):
    """Deletes the LoadTest (its Jobs and pods go with it). ?results=true also deletes its run folder in S3."""
    _watched(ns)
    deleted = {"deleted": name}
    if request.args.get("results", "").lower() in ("1", "true", "yes"):
        st = runs.backend().get_loadtest(ns, name).get("status") or {}
        if (st.get("phase") or "Pending") not in TERMINAL:
            return jsonify(error="the run is still active - stop it first (Delete), then delete its results from History"), 409
        folder = _results_folder(st.get("runDir"))
        if folder:
            deleted["resultObjects"] = storage.delete_run(folder)   # results first: if this fails the run stays visible
    runs.backend().delete_loadtest(ns, name)
    return deleted


@bp.delete("/runs/<folder>")
def delete_run(folder):
    """Deletes a run's folder in S3 (logs, report, summary, test copy). Refused while its LoadTest is still active."""
    if not _NAME.match(folder):
        abort(404)
    url = storage.run_folder_url(folder)
    for lt in runs.backend().list_loadtests(config.WATCH_NAMESPACES):
        st = lt.get("status") or {}
        if st.get("runDir") == url and (st.get("phase") or "Pending") not in TERMINAL:
            return jsonify(error=f"run {lt['metadata']['name']} is still active - stop it first"), 409
    n = storage.delete_run(folder)
    if n == 0:
        return jsonify(error=f"no results for {folder}"), 404
    return {"deleted": folder, "objects": n}


@bp.get("/runs")
def list_runs():
    return jsonify(storage.list_runs())
