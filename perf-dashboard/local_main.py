"""perf-dashboard on one machine: no Kubernetes, no Docker, no MinIO - only Java 17+ and Python 3.

Tests run as local perf processes (validate -> setup -> N workers -> report); results and test plans live in a
folder. The same UI and API as in the cluster.

  python local_main.py                      # http://127.0.0.1:5000, data in ./perf-data
  python local_main.py --port 8080 --data D:\\perf-data --perf D:\\tools\\perf-1.0

Settings (flags or environment): PERF_HOME (the unzipped perf distribution: bin/, lib/, conf/), PERF_JAVA / JAVA_HOME,
STORAGE_DIR (data folder), HOST (default 127.0.0.1 - the dashboard has no login, keep it local), PORT,
LOCAL_MAX_WORKERS (default 8).
"""
import argparse
import os
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _perf_home(given):
    candidates = [given, os.getenv("PERF_HOME"), HERE / "perf",
                  *sorted(HERE.parent.glob("gatling-runner/target/perf-*/perf-*"), reverse=True)]
    for c in candidates:
        if c and (Path(c) / "lib").is_dir():
            return str(Path(c).resolve())
    sys.exit("perf distribution not found - pass --perf <folder with lib/> or set PERF_HOME "
             "(build it with: mvn -pl gatling-runner -am package)")


def _java(given):
    for c in (given, os.getenv("PERF_JAVA"),
              os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.getenv("JAVA_HOME") else None,
              shutil.which("java")):
        if c and (os.path.isfile(c) or os.path.isfile(c + ".exe") or shutil.which(c)):
            return c
    sys.exit("Java 17+ not found - install it, or set JAVA_HOME / PERF_JAVA")


def main():
    ap = argparse.ArgumentParser(description="perf-dashboard without Kubernetes")
    ap.add_argument("--host", default=os.getenv("HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("PORT", "5000")))
    ap.add_argument("--data", default=os.getenv("STORAGE_DIR", str(Path.cwd() / "perf-data")), help="results and test plans")
    ap.add_argument("--perf", default=None, help="the unzipped perf distribution")
    ap.add_argument("--java", default=None)
    a = ap.parse_args()

    # must be set before config is imported
    os.environ.update({
        "EXECUTOR": "local",
        "STORAGE": "folder",
        "STORAGE_DIR": str(Path(a.data).resolve()),
        "WATCH_NAMESPACES": os.getenv("WATCH_NAMESPACES", "local"),
        "PERF_HOME": _perf_home(a.perf),
        "PERF_JAVA": _java(a.java),
    })
    sys.path.insert(0, str(HERE))
    from app import create_app
    from waitress import serve

    print(f"perf-dashboard (local): http://{a.host}:{a.port}")
    print(f"  data:  {os.environ['STORAGE_DIR']}")
    print(f"  perf:  {os.environ['PERF_HOME']}")
    print(f"  java:  {os.environ['PERF_JAVA']}")
    if a.host not in ("127.0.0.1", "localhost"):
        print("  NOTE: listening beyond this machine - the dashboard has no login")
    serve(create_app(), host=a.host, port=a.port, threads=8)


if __name__ == "__main__":
    main()
