"""Where runs are executed: Kubernetes (LoadTest resources + the operator) or this machine (local perf processes).

Both backends offer the same calls - list_loadtests, get_loadtest, create_loadtest, delete_loadtest, jobs, pods,
pod_logs - and return the same shapes (LoadTest-like dicts, described jobs and pods), so the API and the UI are the
same in both modes.
"""
import config

GROUP, VERSION = "perf.codelabuk.org", "v1alpha1"


class RunError(Exception):
    """A request the run backend refuses - shown to the user with its HTTP status."""

    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def backend():
    if config.EXECUTOR == "local":
        import local_runs
        return local_runs
    import kube  # only needed (and only importable) with the kubernetes client installed
    return kube
