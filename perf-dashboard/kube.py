"""Everything the dashboard asks Kubernetes: LoadTests, their Jobs and pods, pod logs."""
from kubernetes import client, config as kconfig

import config

GROUP, VERSION, PLURAL = "perf.codelabuk.org", "v1alpha1", "loadtests"
LABEL_LOADTEST = "perf.codelabuk.org/loadtest"
LABEL_ROLE = "perf.codelabuk.org/role"

_clients = {}


def _api(kind):
    if not _clients:
        try:
            kconfig.load_incluster_config()
        except kconfig.ConfigException:
            kconfig.load_kube_config(context=config.KUBE_CONTEXT)
        _clients["core"] = client.CoreV1Api()
        _clients["batch"] = client.BatchV1Api()
        _clients["custom"] = client.CustomObjectsApi()
    return _clients[kind]


def list_loadtests(namespaces):
    items = []
    for ns in namespaces:
        items += _api("custom").list_namespaced_custom_object(GROUP, VERSION, ns, PLURAL).get("items", [])
    return items


def get_loadtest(ns, name):
    return _api("custom").get_namespaced_custom_object(GROUP, VERSION, ns, PLURAL, name)


def create_loadtest(ns, body):
    return _api("custom").create_namespaced_custom_object(GROUP, VERSION, ns, PLURAL, body)


def delete_loadtest(ns, name):
    # foreground: Kubernetes removes the run's Jobs and pods (owned by the LoadTest) first
    return _api("custom").delete_namespaced_custom_object(
        GROUP, VERSION, ns, PLURAL, name, body=client.V1DeleteOptions(propagation_policy="Foreground"))


def jobs_for(ns, name):
    return _api("batch").list_namespaced_job(ns, label_selector=f"{LABEL_LOADTEST}={name}").items


def pods_for(ns, name):
    return _api("core").list_namespaced_pod(ns, label_selector=f"{LABEL_LOADTEST}={name}").items


def pod_logs(ns, pod, tail):
    return _api("core").read_namespaced_pod_log(pod, ns, container="perf", tail_lines=tail)


def describe_job(j):
    s = j.status
    return {
        "name": j.metadata.name,
        "role": (j.metadata.labels or {}).get(LABEL_ROLE),
        "completions": j.spec.completions,
        "succeeded": s.succeeded or 0,
        "failed": s.failed or 0,
        "active": s.active or 0,
        "start": s.start_time.isoformat() if s.start_time else None,
        "end": s.completion_time.isoformat() if s.completion_time else None,
    }


def describe_pod(p):
    labels = p.metadata.labels or {}
    annotations = p.metadata.annotations or {}
    statuses = p.status.container_statuses or []
    perf = next((c for c in statuses if c.name == "perf"), None)
    terminated = perf.state.terminated if perf and perf.state else None
    index = labels.get("batch.kubernetes.io/job-completion-index") or annotations.get("batch.kubernetes.io/job-completion-index")
    return {
        "name": p.metadata.name,
        "role": labels.get(LABEL_ROLE),
        "worker": int(index) if index is not None and str(index).isdigit() else None,
        "node": p.spec.node_name,
        "podIP": p.status.pod_ip,
        "phase": p.status.phase,
        "ready": bool(perf and perf.ready),
        "restarts": perf.restart_count if perf else 0,
        "start": p.status.start_time.isoformat() if p.status.start_time else None,
        "end": terminated.finished_at.isoformat() if terminated and terminated.finished_at else None,
        "exitCode": terminated.exit_code if terminated else None,
        "reason": (terminated.reason if terminated else None) or (perf.state.waiting.reason if perf and perf.state and perf.state.waiting else None),
    }


def jobs(ns, name):
    return [describe_job(j) for j in jobs_for(ns, name)]


def pods(ns, name):
    return [describe_pod(p) for p in pods_for(ns, name)]
