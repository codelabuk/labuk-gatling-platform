"""Runs LoadTests on this machine, without Kubernetes: the same perf commands the operator's Jobs run, as processes.

  Validating   perf validate <plan> --workers N --facts-file ...
  Setup        perf setup <plan> --out <run>/shared.json              (only if the plan shares lists)
  Running      N x perf worker (WORKER_INDEX, WORKER_COUNT, START_AT, RESULTS_DIR=<run>/logs, METRICS_PORT per worker)
  Aggregating  perf report --input <run>/logs --output <run>/report --name merged --facts-file ...
  Completed / Failed

The run folder has the same layout as in MinIO (tests/, shared.json, logs/, report/), under the storage folder, so
summaries, reports, History and deletion work unchanged. Run state and process logs live in LOCAL_STATE_DIR. Each
"pod" of the UI is one process; the functions below return the same shapes as kube.py.
"""
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import config
from runs import RunError

TERMINAL = {"Completed", "Failed"}
_lock = threading.RLock()
_runs = {}        # (ns, name) -> Run
_loaded = False


def _now():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.isoformat().replace("+00:00", "Z") if dt else None


def _split_s3(url):
    rest = url[len("s3://"):] if url.startswith("s3://") else url
    bucket, _, key = rest.partition("/")
    return bucket, key.strip("/")


def _folder(url):
    """s3://bucket/key -> the matching folder under STORAGE_DIR (the folder storage's layout)."""
    from folder_store import FolderClient
    bucket, key = _split_s3(url)
    return FolderClient(config.STORAGE_DIR).path(bucket, key)


def _heap(memory):
    """Kubernetes memory ("1Gi", "512Mi", "2G") -> -Xmx of 75% of it, like the runner image; default 1g."""
    if not memory:
        return "1g"
    units = {"Ki": 1 / 1024, "Mi": 1, "Gi": 1024, "K": 1 / 1000, "M": 1, "G": 1000}
    for u, f in sorted(units.items(), key=lambda kv: -len(kv[0])):
        if memory.endswith(u):
            mb = float(memory[: -len(u)]) * f
            return f"{max(256, int(mb * 0.75))}m"
    return "1g"


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------- processes


class Proc:
    """One process of a run - shown in the UI like a pod."""

    def __init__(self, run, role, worker=None):
        self.run, self.role, self.worker = run, role, worker
        self.name = f"{run.name}-{role}" + (f"-{worker}" if worker is not None else "")
        self.log = run.state_dir / f"{self.name}.log"
        self.popen = None
        self.start = self.end = None
        self.exit_code = None
        self.port = None

    def launch(self, args, env=None):
        cmd = [config.PERF_JAVA, f"-Xmx{_heap(self.run.memory)}",
               "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
               f"-Dlogback.configurationFile={Path(config.PERF_HOME, 'conf', 'logback.xml')}",
               "-cp", os.pathsep.join([str(Path(config.PERF_HOME, "lib")) + os.sep + "*",
                                       str(Path(config.PERF_HOME, "drivers")) + os.sep + "*"]),
               "org.codelabuk.perf.runner.Perf", *args]
        full_env = {**os.environ, **self.run.env, **(env or {})}
        out = open(self.log, "ab")
        kwargs = {"stdout": out, "stderr": subprocess.STDOUT, "env": full_env, "cwd": str(self.run.folder)}
        if sys.platform == "win32":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True  # its own process group: stop() ends perf and any JVM it started
        self.start = _now()
        try:
            self.popen = subprocess.Popen(cmd, **kwargs)
        except FileNotFoundError:
            out.close()
            raise RunError(f"Java not found at {config.PERF_JAVA} - set PERF_JAVA (or JAVA_HOME)", 500)
        self._out = out

    def wait(self, timeout=None):
        try:
            code = self.popen.wait(timeout)
        except subprocess.TimeoutExpired:
            return None
        self._finish(code)
        return code

    def _finish(self, code):
        if self.end is None:
            self.exit_code, self.end = code, _now()
            self._out.close()

    def poll(self):
        if self.popen is not None and self.end is None:
            code = self.popen.poll()
            if code is not None:
                self._finish(code)

    def stop(self):
        """Ends the process and its children (perf may restart itself as a child JVM)."""
        if self.popen is None or self.popen.poll() is not None:
            return
        try:
            if sys.platform == "win32":
                subprocess.run(["taskkill", "/T", "/F", "/PID", str(self.popen.pid)], capture_output=True)
            else:
                os.killpg(self.popen.pid, signal.SIGTERM)
            self.popen.wait(10)
        except Exception:  # noqa: BLE001 - best effort; kill below
            self.popen.kill()
        self._finish(self.popen.poll())

    @property
    def phase(self):
        self.poll()
        if self.popen is None:
            return "Pending"
        if self.end is None:
            return "Running"
        return "Succeeded" if self.exit_code in (0, 2) else "Failed"   # 2 = this worker's SLAs (judged on the merge)

    def describe(self):
        p = self.phase
        return {"name": self.name, "role": self.role, "worker": self.worker, "node": socket.gethostname(),
                "podIP": "127.0.0.1", "metricsPort": self.port, "phase": p, "ready": p == "Running", "restarts": 0,
                "start": _iso(self.start), "end": _iso(self.end), "exitCode": self.exit_code,
                "reason": None if p in ("Running", "Pending", "Succeeded") else f"exit code {self.exit_code}"}


# ---------------------------------------------------------------------- runs


class Run:
    def __init__(self, lt):
        self.lt = lt
        self.ns, self.name = lt["metadata"]["namespace"], lt["metadata"]["name"]
        spec = lt["spec"]
        self.spec = spec
        self.env = {k: str(v) for k, v in (spec.get("env") or {}).items()}
        self.memory = ((spec.get("resources") or {}).get("limits") or {}).get("memory")
        self.state_dir = Path(config.LOCAL_STATE_DIR) / "logs" / f"{self.ns}__{self.name}"
        self.procs = []
        self.stopping = threading.Event()

    @property
    def status(self):
        return self.lt.setdefault("status", {})

    @property
    def folder(self):
        return _folder(self.status["runDir"])

    def update(self, **fields):
        with _lock:
            self.status.update({k: v for k, v in fields.items()})
            _save(self)

    def fail(self, message):
        self.update(phase="Failed", message=message, finishedAt=_iso(_now()))

    def proc(self, role, worker=None):
        p = Proc(self, role, worker)
        with _lock:
            self.procs.append(p)
        return p


def _state_file(ns, name):
    return Path(config.LOCAL_STATE_DIR) / "runs" / f"{ns}__{name}.json"


def _save(run):
    f = _state_file(run.ns, run.name)
    f.parent.mkdir(parents=True, exist_ok=True)
    tmp = f.with_suffix(".tmp")
    tmp.write_text(json.dumps(run.lt, indent=2), encoding="utf-8")
    os.replace(tmp, f)


def _load():
    """Runs from earlier sessions. A run that was active when the dashboard stopped cannot continue (its processes
    ended with it): it is marked Failed; its results so far stay in the run folder."""
    global _loaded
    with _lock:
        if _loaded:
            return
        _loaded = True
        for f in sorted((Path(config.LOCAL_STATE_DIR) / "runs").glob("*.json")):
            try:
                lt = json.loads(f.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            run = Run(lt)
            if (run.status.get("phase") or "Pending") not in TERMINAL:
                run.status.update(phase="Failed", message="the dashboard stopped while this run was active", finishedAt=_iso(_now()))
                _save(run)
            _runs[(run.ns, run.name)] = run


def _execute(run):
    """Validate -> setup -> workers -> report, as the operator does with Jobs."""
    spec, st = run.spec, run.status
    try:
        folder = run.folder
        (folder / "logs").mkdir(parents=True, exist_ok=True)
        facts_dir = run.state_dir / "facts"
        facts_dir.mkdir(parents=True, exist_ok=True)

        # the tests: a copy of the suite in the run folder (like the validate pod's snapshot)
        plan_cfg = spec.get("plan") or {}
        if plan_cfg.get("s3"):
            suite = _folder(plan_cfg["s3"])
            if not suite.is_dir():
                return run.fail(f"test plan suite not found: {plan_cfg['s3']}")
            shutil.copytree(suite, folder / "tests", dirs_exist_ok=True)
            plan = folder / "tests" / plan_cfg["file"]
        elif plan_cfg.get("local"):
            plan = Path(plan_cfg["file"])
        else:
            return run.fail("local runs take their tests from saved test plans (plan.s3) or a local plan file (plan.local)")
        if not plan.is_file():
            return run.fail(f"plan file not found: {plan_cfg['file']}")
        workers = int(spec.get("workers", 1))
        scenarios = [s for s in (spec.get("scenarios") or []) if s]

        # 1. validate
        run.update(phase="Validating", message="validating the plan")
        v = run.proc("validate")
        args = ["validate", str(plan), "--workers", str(workers), "--facts-file", str(facts_dir / "validate.json")]
        if scenarios:
            args += ["--scenario", ",".join(scenarios)]
        v.launch(args)
        v.wait()
        if run.stopping.is_set():
            return
        facts = _facts(facts_dir / "validate.json")
        if not facts.get("valid"):
            return run.fail("plan is invalid: " + (facts.get("message") or "no validation result - see the validate log"))
        run.update(plan=facts.get("plan"), hasSetup=bool(facts.get("hasSetup")), scenarios=facts.get("scenarios") or [])

        # 2. setup (once)
        shared = None
        if facts.get("hasSetup"):
            run.update(phase="Setup", message="running the plan's setup once")
            s = run.proc("setup")
            shared = folder / "shared.json"
            s.launch(["setup", str(plan), "--out", str(shared)])
            code = s.wait()
            if run.stopping.is_set():
                return
            if code != 0:
                return run.fail(f"setup failed (exit code {code}) - see the setup log")

        # 3. workers, all starting at the same moment
        delay = min(int(spec.get("startDelaySeconds") or 5), config.LOCAL_START_DELAY_MAX)
        start_at = _now() + timedelta(seconds=delay)
        metrics = (spec.get("metrics") or {}).get("enabled", True)
        linger = (spec.get("metrics") or {}).get("lingerSeconds", 30)
        run.update(phase="Running", message=f"{workers} worker(s) start together at {_iso(start_at)}",
                   startAt=_iso(start_at), startedAt=_iso(_now()), workersSucceeded=0, workersFailed=0)
        procs = []
        for i in range(workers):
            w = run.proc("workers", i)
            env = {"PLAN": str(plan), "WORKER_INDEX": str(i), "WORKER_COUNT": str(workers), "START_AT": _iso(start_at),
                   "RESULTS_DIR": str(folder / "logs")}
            if shared:
                env["SHARED_IN"] = str(shared)
            if scenarios:
                env["SCENARIO"] = ",".join(scenarios)
            if metrics:
                w.port = _free_port()
                env.update(METRICS_PORT=str(w.port), METRICS_LINGER_SECONDS=str(linger))
            w.launch(["worker"], env)
            procs.append(w)
        deadline = time.time() + int(spec.get("timeoutMinutes") or 180) * 60 + delay + (linger if metrics else 0)
        while any(p.phase == "Running" for p in procs):
            if run.stopping.is_set():
                return
            if time.time() > deadline:
                for p in procs:
                    p.stop()
                return run.fail("workers did not finish in time (timeoutMinutes)")
            ok = sum(p.phase == "Succeeded" for p in procs)
            bad = sum(p.phase == "Failed" for p in procs)
            if (ok, bad) != (st.get("workersSucceeded"), st.get("workersFailed")):
                run.update(workersSucceeded=ok, workersFailed=bad)
            time.sleep(1)
        ok = sum(p.phase == "Succeeded" for p in procs)
        failed = [p for p in procs if p.phase == "Failed"]
        run.update(workersSucceeded=ok, workersFailed=len(failed))
        if failed:
            return run.fail(f"worker {failed[0].worker} failed (exit code {failed[0].exit_code}) - see its log")

        # 4. merge + report
        run.update(phase="Aggregating", message=f"merging {workers} worker log(s) into one report")
        r = run.proc("report")
        r.launch(["report", "--input", str(folder / "logs"), "--output", str(folder / "report"), "--name", "merged",
                  "--facts-file", str(facts_dir / "report.json")])
        r.wait()
        if run.stopping.is_set():
            return
        rf = _facts(facts_dir / "report.json")
        if not rf.get("reportBuilt"):
            return run.fail("the report could not be built - see the report log")
        sla = bool(rf.get("slaPassed"))
        run.update(phase="Completed", result="Passed" if sla else "SLAFailed",
                   message="all SLAs met" if sla else "SLAs failed - see the report",
                   reportPath=f"{st['runDir']}/report/merged/index.html", finishedAt=_iso(_now()))
    except RunError as e:
        run.fail(str(e))
    except Exception as e:  # noqa: BLE001 - a broken run must not stay "Running" forever
        run.fail(f"{type(e).__name__}: {e}")


def _facts(f):
    try:
        return json.loads(Path(f).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


# ---------------------------------------------------------------------- the backend calls (same as kube.py)


def list_loadtests(namespaces):
    _load()
    with _lock:
        return [r.lt for (ns, _), r in sorted(_runs.items()) if ns in namespaces]


def _get(ns, name):
    _load()
    run = _runs.get((ns, name))
    if run is None:
        raise RunError(f"run {name} not found", 404)
    return run


def get_loadtest(ns, name):
    return _get(ns, name).lt


def create_loadtest(ns, body):
    _load()
    name = body["metadata"]["name"]
    spec = body["spec"]
    workers = int(spec.get("workers", 1))
    if workers > config.LOCAL_MAX_WORKERS:
        raise RunError(f"local runs use at most {config.LOCAL_MAX_WORKERS} worker processes on this machine "
                       f"(LOCAL_MAX_WORKERS) - {workers} asked")
    results = ((spec.get("results") or {}).get("s3") or {}).get("url") or config.RESULTS_URL
    uid = str(uuid.uuid4())
    lt = {**body, "metadata": {**body["metadata"], "namespace": ns, "uid": uid, "creationTimestamp": _iso(_now())},
          "status": {"phase": "Pending", "message": "starting",
                     "runDir": f"{results.rstrip('/')}/{name}-{uid.replace('-', '')[:8]}"}}
    with _lock:
        if (ns, name) in _runs:
            raise RunError(f"a run named {name} already exists", 409)
        run = Run(lt)
        run.state_dir.mkdir(parents=True, exist_ok=True)
        _runs[(ns, name)] = run
        _save(run)
    threading.Thread(target=_execute, args=(run,), name=f"run-{name}", daemon=True).start()
    return lt


def delete_loadtest(ns, name):
    """Stops the run's processes and forgets the run; its results stay in the run folder (like a deleted LoadTest)."""
    run = _get(ns, name)
    run.stopping.set()
    for p in list(run.procs):
        p.stop()
    with _lock:
        _runs.pop((ns, name), None)
        _state_file(ns, name).unlink(missing_ok=True)
    shutil.rmtree(run.state_dir, ignore_errors=True)


def jobs(ns, name):
    run = _get(ns, name)
    out = []
    for role in ("validate", "setup", "workers", "report"):
        ps = [p for p in run.procs if p.role == role]
        if not ps:
            continue
        phases = [p.phase for p in ps]
        starts = [p.start for p in ps if p.start]
        ends = [p.end for p in ps if p.end]
        done = all(ph in ("Succeeded", "Failed") for ph in phases)
        out.append({"name": f"{name}-{role}", "role": role, "completions": len(ps),
                    "succeeded": phases.count("Succeeded"), "failed": phases.count("Failed"), "active": phases.count("Running"),
                    "start": _iso(min(starts)) if starts else None, "end": _iso(max(ends)) if done and ends else None})
    return out


def pods(ns, name):
    return [p.describe() for p in _get(ns, name).procs]


def pod_logs(ns, pod, tail):
    for run in list(_runs.values()):
        if run.ns != ns:
            continue
        f = run.state_dir / f"{pod}.log"
        if f.is_file():
            lines = f.read_text(encoding="utf-8", errors="replace").splitlines()
            return "\n".join(lines[-tail:])
    raise RunError(f"no log for {pod}", 404)
