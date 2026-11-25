"""Live numbers of a running LoadTest: scrape every worker's Prometheus endpoint and sum them up."""
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests

import config

_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+([-+]?[0-9.eE+-]+|NaN|[+-]?Inf)\s*$')
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse(text):
    """Prometheus text format -> list of (metric, labels dict, value)."""
    out = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _LINE.match(line.strip())
        if not m:
            continue
        labels = {k: v.replace('\\"', '"').replace("\\\\", "\\") for k, v in _LABEL.findall(m.group(3) or "")}
        out.append((m.group(1), labels, float(m.group(4))))
    return out


def summarize(samples):
    """One worker's samples -> counters by request, histogram buckets, users."""
    requests_ = {}
    users = 0.0
    errors = {}
    for name, labels, value in samples:
        if name == "perf_requests_total":
            r = requests_.setdefault(labels.get("request", "?"), {"ok": 0.0, "ko": 0.0, "buckets": {}, "sum": 0.0, "count": 0.0})
            r["ok" if labels.get("status") == "OK" else "ko"] += value
        elif name == "perf_response_time_ms_bucket":
            r = requests_.setdefault(labels.get("request", "?"), {"ok": 0.0, "ko": 0.0, "buckets": {}, "sum": 0.0, "count": 0.0})
            le = labels.get("le", "+Inf")
            r["buckets"][math.inf if le == "+Inf" else float(le)] = value
        elif name == "perf_response_time_ms_sum":
            requests_.setdefault(labels.get("request", "?"), {"ok": 0.0, "ko": 0.0, "buckets": {}, "sum": 0.0, "count": 0.0})["sum"] = value
        elif name == "perf_response_time_ms_count":
            requests_.setdefault(labels.get("request", "?"), {"ok": 0.0, "ko": 0.0, "buckets": {}, "sum": 0.0, "count": 0.0})["count"] = value
        elif name == "perf_users_active":
            users += value
        elif name == "perf_errors_total":
            errors[labels.get("message", "?")] = errors.get(labels.get("message", "?"), 0) + value
    return {"requests": requests_, "usersActive": users, "errors": errors}


def quantile(q, buckets):
    """Like Prometheus' histogram_quantile: linear interpolation inside the bucket holding the q-th value."""
    if not buckets:
        return None
    bounds = sorted(buckets)
    total = buckets[bounds[-1]]
    if total <= 0:
        return None
    rank = q * total
    prev_bound, prev_count = 0.0, 0.0
    for b in bounds:
        count = buckets[b]
        if count >= rank:
            if math.isinf(b):
                return prev_bound          # beyond the last finite bucket: its upper bound is the best estimate
            if count == prev_count:
                return b
            return prev_bound + (b - prev_bound) * (rank - prev_count) / (count - prev_count)
        prev_bound, prev_count = b, count
    return bounds[-2] if len(bounds) > 1 else None


def merge(workers):
    """Sum the per-worker summaries into run totals (+ estimated p95 per request and overall)."""
    total = {"requests": {}, "usersActive": 0.0, "errors": {}}
    for w in workers:
        total["usersActive"] += w["usersActive"]
        for msg, c in w["errors"].items():
            total["errors"][msg] = total["errors"].get(msg, 0) + c
        for name, r in w["requests"].items():
            t = total["requests"].setdefault(name, {"ok": 0.0, "ko": 0.0, "buckets": {}, "sum": 0.0, "count": 0.0})
            t["ok"] += r["ok"]; t["ko"] += r["ko"]; t["sum"] += r["sum"]; t["count"] += r["count"]
            for b, c in r["buckets"].items():
                t["buckets"][b] = t["buckets"].get(b, 0.0) + c
    all_buckets = {}
    for r in total["requests"].values():
        for b, c in r["buckets"].items():
            all_buckets[b] = all_buckets.get(b, 0.0) + c
    total["p95"] = quantile(0.95, all_buckets)
    return total


def as_json(summary):
    """Drop raw buckets, add per-request p95 / mean - what the browser needs."""
    reqs = []
    for name, r in sorted(summary["requests"].items()):
        reqs.append({
            "name": name, "ok": int(r["ok"]), "ko": int(r["ko"]),
            "mean": round(r["sum"] / r["count"]) if r["count"] else None,
            "p95": None if quantile(0.95, r["buckets"]) is None else round(quantile(0.95, r["buckets"])),
        })
    ok = sum(r["ok"] for r in reqs)
    ko = sum(r["ko"] for r in reqs)
    out = {"ok": ok, "ko": ko, "usersActive": int(summary["usersActive"]), "requests": reqs,
           "errors": [{"message": m, "count": int(c)} for m, c in sorted(summary["errors"].items(), key=lambda x: -x[1])][:20]}
    if "p95" in summary:
        out["p95"] = None if summary["p95"] is None else round(summary["p95"])
    return out


# last scrape of every worker, per run: a finished worker can no longer be scraped, but its numbers must keep
# counting - live totals never go down. (One gunicorn process, so every request sees the same memory.)
_last = {}            # run key -> {pod: {"summary", "worker", "seen"}}
_KEEP_SECONDS = 3600


def scrape(pods, port, run_key=None):
    """Scrape the running worker pods in parallel; unreachable ones are reported, not fatal."""
    def one(p):
        try:
            # local runs give every worker its own port; pods all use the run's port
            r = requests.get(f"http://{p['podIP']}:{p.get('metricsPort') or port}/metrics", timeout=config.METRICS_TIMEOUT_SECONDS)
            r.raise_for_status()
            return p, summarize(parse(r.text)), None
        except Exception as e:  # noqa: BLE001 - a slow/finished worker must not break the page
            return p, None, str(e)

    targets = [p for p in pods if p.get("role") == "workers" and p.get("podIP") and p.get("phase") == "Running"]
    with ThreadPoolExecutor(max_workers=max(1, min(16, len(targets)))) as pool:
        results = list(pool.map(one, targets))

    now = time.time()
    for key in [k for k, v in _last.items() if all(now - e["seen"] > _KEEP_SECONDS for e in v.values())]:
        del _last[key]
    known = _last.setdefault(run_key, {}) if run_key else {}
    for p, s, _ in results:
        if s is not None:
            known[p["name"]] = {"summary": s, "worker": p.get("worker"), "seen": now}
    current = {p["name"] for p, s, _ in results if s is not None}

    workers = []
    for pod, e in known.items():
        finished = pod not in current
        summary = dict(e["summary"], usersActive=0.0) if finished else e["summary"]
        workers.append({"pod": pod, "worker": e["worker"], "finished": finished, **as_json(summary)})
    if not run_key:  # no memory: only what was scraped now
        workers = [{"pod": p["name"], "worker": p.get("worker"), "finished": False, **as_json(s)} for p, s, _ in results if s is not None]
    total = merge([dict(e["summary"], usersActive=0.0) if pod not in current else e["summary"] for pod, e in known.items()]
                  if run_key else [s for _, s, _ in results if s is not None])
    return {
        "time": now,
        "workers": sorted(workers, key=lambda w: (w["worker"] is None, w["worker"])),
        "unreachable": [{"pod": p["name"], "error": err} for p, s, err in results if s is None and p["name"] not in known],
        "total": as_json(total),
    }
