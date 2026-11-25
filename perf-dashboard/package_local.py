"""Builds build/perf-dashboard-local.zip: the dashboard for one machine (no Kubernetes, no Docker, no MinIO).

    python package_local.py                 # needs: npm run build (static/dist) and the perf distribution
    python package_local.py --with-wheels   # also the Python packages, for machines without internet / pip mirror

Contents: the backend, the built UI, requirements-local.txt, start.cmd / start.sh, README-local.md and perf/ (the
unzipped perf distribution: what runs the tests). The target machine needs only Java 17+ and Python 3.10+.
"""
import argparse
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
APP_FILES = ["app.py", "config.py", "folder_store.py", "live.py", "local_main.py", "local_runs.py", "lr_import.py",
             "plan_store.py", "runs.py", "storage.py", "kube.py", "requirements-local.txt", "start.cmd", "start.sh"]

README = """# perf-dashboard (local)

Runs load tests on this machine - no Kubernetes, no Docker, no MinIO. Needs **Java 17+** and **Python 3.10+**.

    start.cmd            (Windows)      ./start.sh      (Linux / macOS)

Then open http://127.0.0.1:5000. The first start creates `.venv` and installs the Python packages (from `wheels/`
when present, otherwise from pip). Results and test plans are kept in `perf-data/` (change with `--data <folder>`).

Options: `--port 8080`, `--data D:\\perf-data`, `--host 0.0.0.0` (other machines can reach it - the dashboard has no
login), `--java <path>`. Environment: `LOCAL_MAX_WORKERS` (default 8 worker processes per run).

Every worker of a run is a process on this machine sharing its CPU: use it to build and try test plans and for
small / medium tests; big distributed tests belong on the Kubernetes dashboard. Stopping the dashboard stops the
tests that are running (their results so far stay in the data folder).
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--perf", help="the unzipped perf distribution (default: ../gatling-runner/target/perf-*/perf-*)")
    ap.add_argument("--with-wheels", action="store_true", help="bundle the Python packages for offline installs")
    ap.add_argument("--out", default=str(HERE / "build" / "perf-dashboard-local.zip"))
    a = ap.parse_args()

    dist = HERE / "static" / "dist"
    if not (dist / "index.html").is_file():
        sys.exit("the UI is not built - run: cd frontend && npm ci && npm run build")
    perf = Path(a.perf) if a.perf else next(iter(sorted(HERE.parent.glob("gatling-runner/target/perf-*/perf-*"), reverse=True)), None)
    if not perf or not (perf / "lib").is_dir():
        sys.exit("perf distribution not found - build it: mvn -pl gatling-runner -am package (or pass --perf)")

    stage = HERE / "build" / "perf-dashboard-local"
    shutil.rmtree(stage, ignore_errors=True)
    stage.mkdir(parents=True)
    for f in APP_FILES:
        shutil.copy2(HERE / f, stage / f)
    shutil.copytree(HERE / "routes", stage / "routes", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(dist, stage / "static" / "dist")
    shutil.copytree(perf, stage / "perf")
    (stage / "README-local.md").write_text(README, encoding="utf-8")
    if a.with_wheels:
        subprocess.run([sys.executable, "-m", "pip", "download", "-q", "-d", str(stage / "wheels"), "-r", str(HERE / "requirements-local.txt")], check=True)

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(stage.rglob("*")):
            if f.is_file():
                z.write(f, f"perf-dashboard-local/{f.relative_to(stage).as_posix()}")
    print(f"{out} ({out.stat().st_size // (1024 * 1024)} MB)")


if __name__ == "__main__":
    main()
