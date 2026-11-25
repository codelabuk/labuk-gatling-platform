#!/usr/bin/env sh
# perf-dashboard without Kubernetes or Docker - needs Java 17+ and Python 3.10+ on this machine.
#   ./start.sh                 http://127.0.0.1:5000, data in ./perf-data
#   ./start.sh --port 8080 --data /srv/perf-data
set -e
cd "$(dirname "$0")"
if [ ! -x .venv/bin/python ]; then
  echo "First start: setting up the Python environment ..."
  python3 -m venv .venv
  if [ -d wheels ]; then
    .venv/bin/python -m pip install -q --no-index --find-links wheels -r requirements-local.txt
  else
    .venv/bin/python -m pip install -q -r requirements-local.txt
  fi
fi
exec .venv/bin/python local_main.py "$@"
