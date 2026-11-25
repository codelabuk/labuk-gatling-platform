@echo off
rem perf-dashboard without Kubernetes or Docker - needs Java 17+ and Python 3.10+ on this machine.
rem   start.cmd                 http://127.0.0.1:5000, data in .\perf-data
rem   start.cmd --port 8080 --data D:\perf-data
setlocal
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo First start: setting up the Python environment ...
  python -m venv .venv || (echo Python 3.10 or newer is required & exit /b 1)
  if exist wheels (
    .venv\Scripts\python -m pip install -q --no-index --find-links wheels -r requirements-local.txt || exit /b 1
  ) else (
    .venv\Scripts\python -m pip install -q -r requirements-local.txt || exit /b 1
  )
)
.venv\Scripts\python local_main.py %*
