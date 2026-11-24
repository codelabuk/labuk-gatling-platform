# Roadmap

Order of work and what "done" means for each module. Sizes are relative (S/M/L), not estimates.

```
gatling-api-core ──► gatling-sql ──┐
       │                           ├──► gatling-runner ──► loadtest-operator ──► platform (Grafana, storage)
       └── hardening (v1.1) ───────┘
```

| # | Module | Status | Size |
|---|---|---|---|
| 1 | `gatling-api-core` v1.1 — hardening | ✅ done, incl. end-to-end test of every step type (`FeatureRunSpec`) | S–M |
| 2 | `gatling-sql` | ✅ done incl. `batch` (verified on Derby; Oracle not yet tried on a real DB) | M |
| 2b | `gatling-flight` | ✅ done (2026-11-22) — Arrow Flight reads / writes, verified against labukDB locally (170 requests, 0 KO, 20 DoPuts committed) and on 2 Kubernetes workers via the dashboard (Passed, 85 + 85) | M |
| 3 | `gatling-runner` | ✅ done: `perf` incl. `submit`/`status`, `s3://` results (verified on MinIO), Dockerfile image (verified) | M |
| 4 | `loadtest-operator` | ✅ done — **verified on a real cluster** (minikube, Helm-installed); S3 results, printer columns, leader election added after; tests from S3 suites with a per-run copy (`plan.s3`) | L |
| 6 | `perf-dashboard` (web UI) | ✅ v1 — verified on Docker Desktop Kubernetes: runs started from it, live per-worker metrics, final summaries, reports from MinIO; **test plans kept in MinIO** (upload, edit, run) and deleting runs / results | M |
| 5 | Platform — live metrics, result storage | ✅ live metrics + Grafana dashboard; S3/MinIO results | S–M |

### Status — 2026-11-25: worker spreading, report sizing, run summary fix

- **Workers spread over nodes**: `spec.spread` soft (default, topology spread, never blocks) / hard (pod
  anti-affinity, one per node) / none; on the dashboard's New run form. Verified on the 1-node cluster: hard kept the
  2nd worker Pending ("didn't match pod anti-affinity rules"), soft ran both (Passed).
- **Report pod resources**: `spec.reportResources` (dashboard: Report memory).
- **Fixed: run summaries of logs over 16 MB covered only the first 16 MB** (`summary.json`, `worker-N.json`, the
  dashboard's final numbers; the HTML report was right). `RunSummary` now reads the whole log in chunks and keeps
  running t-digests per name instead of every response time - memory stays flat, numbers unchanged.
- **Capacity test (`plans/capacity.yaml`, stepped arrival rate)** on Docker Desktop (4 CPUs, generator and mock on
  the same machine): about 300-650 req/s sustained before arrivals outran responses (active users grew to ~60k,
  p95 hit the 60 s timeout). The limit there is the shared laptop, not a clean per-worker number - repeat it on the
  company cluster with the mock on other nodes.
- Seen: a worker that cannot be scheduled (e.g. "Insufficient cpu") keeps the run in Running until its timeout - the
  dashboard should show the pod's scheduling message (open).

### Status — 2026-11-22: Arrow Flight

New module **`gatling-flight`**: `flight` steps — `query` (GetFlightInfo, then DoGet on all / the first / no endpoints,
several in parallel; checks on rows / endpoints / batches / bytes; extract values, rows, counts) and `put` (DoPut of
Arrow batches built from YAML or session rows; schema from the server's GetSchema or given). 13 tests incl. a real
Gatling run against an in-process Flight server shaped like labukDB.

Verified against **labukDB** (`labukdb-server-app`, Arrow Flight 16.1.0) with a Delta table created by labukDB's own
library (9 files, 9,000 rows): partition scan with 3 endpoints in parallel, server-side GROUP BY / ORDER BY,
projection + filter, concurrent DoPuts — `perf run`: 170 requests, 0 KO, SLAs passed, every put committed (one new
file each); through the dashboard on 2 Kubernetes workers (pods → `host.docker.internal:9443`): Completed / Passed,
170 requests (85 + 85), 0 KO.

Found and fixed on the way:
- **Arrow Flight 16.1 + Gatling's Netty broke HTTP/2 streams** (`http2 exception`, `end-of-stream mid-frame`): Flight
  brings gRPC 1.63 (built for Netty 4.1.100) and Netty 4.1.100 / 4.1.108. Fixed in the parent pom: one Netty
  (4.1.115, Gatling's) and gRPC 1.76 for every module; Arrow's Netty buffer patch excluded (`arrow-memory-unsafe`).
  The runner distribution now ships exactly one Netty and one gRPC version.
- **`perf validate` failed for plans with a connection-type data source** (`key not found` - a `flight` server, also
  a `jdbc` connection): the split table now lists only data sources that feed rows.
- **labukDB** (reported, not changed): `GetSchema` returns timestamp columns as `Int64`, while its Delta writer needs
  `Timestamp(MICROSECOND)` without time zone — a client trusting GetSchema cannot write timestamps. Workaround in the
  plan: `schema:` with the new `timestampNtz` type.

### Status — 2026-11-18

73 JVM tests (core 38, SQL 7, runner 16, operator 12) + 50 dashboard tests. Target platform: **on-prem
Kubernetes** (the company uses Argo CD); results and test plans in **MinIO**.

New since 2026-11-17 — **test plans without Git**:
- the dashboard's *Test plans* page keeps suites of test files in `s3://perf-results/test-plans/<suite>/`
  (create with a starter plan, upload files / folder / zip, edit YAML and CSV, download, delete);
- *New run* takes a suite + plan (Git and ConfigMap are no longer offered in the dashboard; the operator still
  accepts them for `perf submit`, CI and GitOps);
- operator `plan.s3`: the validate pod copies the suite into `<run folder>/tests` (`perf fetch --snapshot`), all
  later pods read that copy — editing a plan never changes a running test;
- deleting results: History → Delete, and *also delete results in S3* on a run; refused while the run is active.

Verified on Docker Desktop Kubernetes: the sample suite uploaded as a zip through the dashboard, a 3-worker run
from it Completed / Passed (246 requests = 79 + 79 + 88 per worker; validate made the copy, setup and all
workers read it), report opened through the dashboard; a second run's results delete was refused while it ran,
then removed 50 objects, its report returned 404 and the first run was untouched.

Found: the **Git source's clone init container fails** on Docker Desktop with `detected dubious ownership in
repository at '/tests'` (the tests volume is owned by root, git runs as uid 1000). Not fixed yet (one line:
`git config --global --add safe.directory /tests`); S3 test plans are not affected.

### Status — 2026-11-17

71 tests (core 37, SQL 7, runner 16, operator 11). Target platform: **on-prem Kubernetes**.

Verified on the team's Docker Desktop Kubernetes with the platform's MinIO (`scripts/docker-desktop-e2e.sh`):
- distributed API run (SR flow, 3 workers, results in MinIO): Completed / Passed — 260 requests, 1 intentional KO;
- operator with 2 replicas: one held the Lease and created all Jobs;
- **LoadRunner**: `perf convert-lr` (new) converted a recorded-style VuGen script; the result ran in Gatling
  (every request/transaction OK) and distributed via `perf submit --s3` (3 workers, 36 requests, 0 KO, Passed);
- `perf fetch` (new) downloaded reports from MinIO.

Found and fixed: **Gatling 3.13 deadlocks at start-up when the JVM sees 1 CPU** (its actor pool has one thread
per CPU; the controller blocks that thread waiting for the result writers). This hits 1-CPU containers and pods
with a 1-CPU limit. `perf` now restarts itself with `-XX:ActiveProcessorCount=2` (thread count only - the CPU
limit still applies). Verified: `docker run --cpus=1 ... run` hung before, completes now. The Maven
`gatling:test` path is not covered (developer machines have more cores).

| Open | Needs |
|---|---|
| perf-dashboard: login (OIDC) — needed before shared use, since anyone with the URL can edit plans and delete results; run comparison; PVC results | design decision (or SSO in front: oauth2-proxy) |
| Test plans: edit history (MinIO bucket versioning, or keep N versions), a "Validate" button running the real plan check, syntax-highlighting editor | decision + small change |
| Git source: `safe.directory` fix for the clone init container (fails where the tests volume is owned by root) | one line + test |
| Company adoption: imagePullSecrets / serviceAccount for run pods, operator-wide defaults, namespace-scoped mode, Argo CD health check for `LoadTest` | small changes (see the Argo CD notes) |
| Spreading workers one per node on multi-node clusters (anti-affinity / topology spread) - not built yet | decision + small change |
| Oracle-specific behaviour (REF CURSOR, PL/SQL OUT params, driver settings) - distributed SQL on a real DB | a dev Oracle database (or the Oracle Free container) |
| Grafana dashboard in a real Grafana + Prometheus | a Grafana/Prometheus instance |
| CI examples (`docs/ci/`) in a real Jenkins / GitLab | the team's CI |
| LoadRunner: a real team script through the import (dashboard) / `perf convert-lr`, numbers compared with LoadRunner's | a VuGen script + a LoadRunner run of the same scenario |
| Browser (TruClient-style) load: `browser` steps on Playwright, separate runner image — agreed to do last | design agreed in outline |
| First run on the production on-prem cluster | registry, MinIO bucket + credentials, access |

---

## 1. `gatling-api-core`

**v1 — done:** YAML engine, API catalog, checks/extraction, flow steps, CSV data, setup + shared lists,
auth (login/jwt/static, auto-refresh), open/closed load, throttle, SLAs, validation with exact error locations,
plugin SPI, worker split.
Added after v1: stepped load (`incrementUsersPerSec` / `incrementConcurrentUsers`), rates in `/s`, `/min`, `/h`,
typo checks inside load steps, load generator heap setting (`-Dperf.heap`, default 2g — the Gatling plugin
default is 1 GB). Measured: 5,000 concurrent users on a 4-CPU / 16 GB laptop, 0 errors.

**v1.1 — hardening for distributed runs** — H1–H5 implemented and verified with
`scripts/local-distributed-demo.sh` (setup once → 2 workers, 15/15 split, no overlap → merged report;
the transaction SLA failed on one worker alone and passed on the merged data, as intended).

| Item | Change |
|---|---|
| H1. SLAs judged on the merged result | **Finding:** Gatling 3.13 writes a binary `simulation.log` and its reports-only mode reads exactly one file — it can no longer merge several logs. So the core gets a **log merger** (records re-based to a common start, string cache rebuilt, SLAs kept from the header) and a `Merge` command that merges pod logs, then runs Gatling reports-only on the result → one HTML report + SLA verdict (exit code 2 = SLA failed). Pods keep SLAs in their logs; the launcher treats a pod's SLA result as informational. The merger is tied to the pinned Gatling version's log format and guarded by an end-to-end test. |
| H2. Same setup data on every pod | Shared lists are sorted before slicing. New **setup-only mode** (`-Dmode=setup -Dshared.out=file.json`) runs setup once and writes the full lists; workers started with `-Dshared.in=file.json` skip setup and take their slice from the file. |
| H3. Synchronised start | `-Dstart.at=<epoch millis or ISO-8601>` — each pod waits until then before injecting load. |
| H4. Failed polls visible in the report | A `pollUntil` that gives up records a named KO entry with its `failMessage` (and, with `name:`, an OK entry timing how long the poll took). |
| H5. Validate-only mode | `Validate` command: load + check a plan, read its data, print the per-worker split for `--workers N`; exit 0/1. Reused by runner, CI and operator. |
| Tests | ✅ local two-worker run merged into one report; ✅ unit tests for start time, shared lists, validate. **Open:** end-to-end coverage for `randomSwitch`, `retry`, `forEach`, `repeat`, `if`, `throttle`, closed model, form/raw/file bodies, `static`/`jwt` auth variants. |

**Backlog (v1.2, driven by real plans):** mTLS client certificates, proxy, per-request timeouts, multipart
upload, XML/SOAP (XPath) checks, JSON data files, JSON Schema for IDE autocomplete.

---

## 2. `gatling-sql` — database load testing

Replace JMeter's JDBC Request and LoadRunner's JDBC/ODBC protocol for **any JDBC database**, Oracle first.

```yaml
dataSources:
  ordersDb:
    type: jdbc
    url: ${DB_URL}
    user: ${DB_USER}
    password: ${DB_PASSWORD}
    pool: { size: 50, connectionTimeout: 5s }
    allowWrites: false             # INSERT/UPDATE/DELETE/CALL refused unless true
  customers:                       # DB-backed feeder
    type: jdbc
    datasource: ordersDb
    query: "SELECT customer_id FROM customers WHERE status = 'ACTIVE'"
    strategy: queue

steps:
  - sql:
      datasource: ordersDb
      name: find-orders
      query: "SELECT id, status FROM orders WHERE customer_id = ?"
      params: [ "#{customerId}" ]
      checks: [ { rowCount: ">= 1" } ]
      extract: { orderId: { column: id } }
  - sql: { datasource: ordersDb, update: "UPDATE orders SET status = ? WHERE id = ?", params: [ PAID, "#{orderId}" ] }
  - sql:
      datasource: ordersDb
      call: "{call pkg_orders.place_order(?, ?, ?)}"
      params: [ "#{customerId}", "#{amount}", { out: number, saveAs: newOrderId } ]
```

Work items:
1. HikariCP pool per data source, opened at test start, closed at the end.
2. `sql` step: `query`, `update`, `call` (OUT params, REF CURSOR), `batch`; parameter types; fetch size; commit/rollback.
3. Dedicated thread pool sized to the connection pool — JDBC blocks and must never run on Gatling's threads.
4. Each statement reported as a request (timings, OK/KO, SLAs); optional separate "connection wait" timing.
5. Checks (`rowCount`, column equals) and extraction (one row / all rows).
6. JDBC feeder.
7. Drivers: Oracle `ojdbc11`, PostgreSQL, SQL Server, MySQL bundled in the runner; others via `drivers/`.
8. Pool budget warning: `pool size × workers` vs. an allowed maximum of DB sessions.

Tests: Derby in-memory (chosen over H2 because it supports procedures with OUT parameters); optional
Oracle Free container test.
**Done when** mixed API+SQL and pure SQL plans run under load against Derby and Oracle with SQL timings and SLAs in the report.

**Status (v1):** items 1–6 and 8 done — pools, `query` / `update` / `call` (OUT params incl. `cursor`,
procedure result rows), `rollback`, own thread pool, report entries + optional connection-wait entry,
checks, extraction, JDBC feeder, `allowWrites` guard, `maxSessions` warning, fail-fast on unreachable DB.
Verified: 6 unit tests + an end-to-end run (660 requests, 0 errors) mixing DB-only load and an API → DB flow.
**Open:** `batch` statements; a run against a real **Oracle** (REF CURSOR / PL/SQL OUT behaviour, driver
properties) — needs a database; SQL Server / MySQL drivers (add when needed, no code change).
Core changes it needed: plugin hooks `isFeeder` and `reportNames`; SLF4J pinned to 2.x in the parent pom
(HikariCP's 1.7.x silently disabled all logging).

---

## 3. `gatling-runner` — what testers and pods run

No Maven or IDE for testers; the same commands locally and in Kubernetes.

```
perf validate plans/x.yaml [--workers 10]
perf run      plans/x.yaml [--scenario a,b] [-DNAME=value]
perf report   <folder of simulation.log files>     # merge → one report + SLA verdict
perf worker                                        # pod entrypoint, settings from env
perf submit   plans/x.yaml --workers 10             # creates a LoadTest (after the operator)
```

Work items:
1. Launcher scripts `perf.sh` / `perf.cmd` around `YamlSimulation`, `Validate` and `Merge`.
2. Distribution zip: core + sql + Gatling + drivers + `logback.xml` + `gatling.conf`.
3. Docker image built with Jib (no Docker daemon needed): Temurin 17 JRE, non-root, JVM defaults.
4. Worker mode: plan location, worker index/count, start time, results target from env; `--no-reports`;
   upload `simulation.log`; treat exit code 2 (SLA) as informational on a pod.
5. Report mode: download logs → merge → report → SLA verdict → upload; exit 0/2.
6. ~~Live metrics switch: Graphite data writer enabled by env.~~ **Dropped: Gatling 3.13's open-source edition
   has no Graphite writer** (only console + file; verified in its source and with a listener). Live metrics
   moved to the platform section as a design item.
7. Tuning defaults for large user counts: heap, file descriptors, ephemeral ports.
8. CI examples (Jenkins, GitLab): validate on merge request, run on schedule, fail on SLA.

**Done when** a tester with only Java (or Docker) can validate, run and merge, and the same image runs as a pod.

**Status (v1):** items 1, 2, 4, 5 done and verified from the unzipped distribution against the mock API —
`perf validate`, `perf run`, `perf setup` → 2 × `perf worker` (env-driven, synchronised start, 15/15 split) →
`perf report` (merged report, SLA verdict), bad plan = one-line error + exit 1; `perf.cmd` verified on Windows.
Item 3 (Jib image) configured; build not yet run. Item 6 dropped (see above). **Open:** build + smoke-test the
image, item 7 (OS tuning guidance), item 8 (CI examples).

---

## 4. `loadtest-operator` — distributed runs on Kubernetes

Java 17, Java Operator SDK on the fabric8 client, CRD generated from Java classes, Helm chart.

```yaml
apiVersion: perf.codelabuk.org/v1alpha1
kind: LoadTest
metadata: { name: orders-peak-20261120 }
spec:
  image: registry/labuk-perf-runner:1.2.0
  plan:
    file: plans/orders-peak.yaml
    s3: s3://perf-results/test-plans/orders-api        # a suite from the dashboard's Test plans page
    # alternatives (CI / GitOps): git: { repo, ref, tokenSecret } | configMap (flat, 1 MB) | inImage: true
  workers: 10
  scenarios: [ checkout ]
  env: { BASE_URL: https://sit.api }
  secretRefs: [ perf-db-creds ]
  resources: { limits: { cpu: "2", memory: 4Gi } }
  nodeSelector: { pool: perf }
  results: { s3: { url: s3://perf-results/runs, endpoint: http://minio.minio.svc:9000, credentialsSecret: minio-creds } }
  metrics: { enabled: true }
  startDelaySeconds: 30
  ttlSecondsAfterFinished: 3600
```

| Phase | Operator action |
|---|---|
| `Validating` | Job runs `perf validate --workers N`; a bad plan fails before any load |
| `Setup` | If the plan has `setup`: one Job runs setup once and publishes the shared lists (H2) |
| `Starting` | Indexed Job with N workers; when all pods are ready, sets `startAt` (H3) |
| `Running` | Watches pods; crash policy (fail fast / continue); timeout cancels |
| `Aggregating` | Job runs `perf report`: merged report + SLA verdict (H1) |
| `Completed` / `Failed` | `status`: report URL, SLA result, duration, per-worker summary; cleanup after TTL |

Work items: CRD + status, reconciler, Job/Pod templates, RBAC, leader election, cancel by delete,
events + `kubectl get loadtests` columns, `perf submit` / `perf status`, Helm chart.
Tests: JOSDK test utilities; end-to-end on kind/k3d against the mock API.
**Done when** `perf submit --workers 5` runs the sample flow on 5 pods with one start time, no item processed
twice, and one merged report with the right SLA verdict.

**Status (v1):** Java 17, JOSDK 5.1.1 / fabric8 7.3.1. `LoadTest` CRD (`perf.codelabuk.org/v1alpha1`, short
name `lt`) generated from Java; lifecycle Validating → Setup → Starting → Running → Aggregating →
Completed/Failed with facts passed back through termination messages (`perf validate/report --facts-file`);
plan from an S3 suite (copied into the run folder by the validate pod) / Git / ConfigMap / image; results in
S3/MinIO or on an existing PVC; owner references, TTL, no retries, non-root;
manifests in `deploy/`. Verified: 9 tests (whole lifecycle against fabric8's mock API server, failure paths,
idempotency, pod details, manifests parse) + the runner's env-driven worker path (`JOB_COMPLETION_INDEX`).
**Not done / open:**
- ✅ **Verified on a real cluster** (minikube, Kubernetes 1.33, operator installed with the Helm chart): `scripts/minikube-e2e.sh` ran a 2-worker LoadTest Validating → Setup → Running → Aggregating → Completed (`result: Passed`) and copied the merged report back. Images are built with plain `docker build` (Dockerfiles in `gatling-runner/`, `loadtest-operator/`, `gatling-api-core/src/test/docker/`) because Jib's hand-off to Docker Desktop stalled on the dev laptop; Jib still works for pushing straight to a registry from CI. On Windows, `minikube image load` needs `wmic` (gone in newer Windows 11), so the script streams images with `docker save | docker exec -i minikube docker load`.
- Start time is set when the workers Job is created (now + `startDelaySeconds`), not when all pods are ready —
  a pod that pulls its image slower than the delay starts late (it logs a warning).
- ✅ Helm chart `charts/loadtest-operator` (CRD in `crds/`, kept identical to the generated one by the build;
  RBAC, Deployment with non-root + read-only root filesystem; values for image, resources, scheduling, log level).
- ✅ `perf submit` / `perf status` CLI, printer columns for `kubectl get`, leader election (2 replicas verified:
  one holds the Lease and creates all Jobs).
- ✅ Results in S3/MinIO (`results.s3`) — no shared volume needed; a PVC still works (ReadWriteMany on
  multi-node clusters).
- ✅ Tests from S3 suites (`plan.s3`): `fetch-tests` init container with the runner image; validate snapshots
  the suite into `<run folder>/tests`, later pods read the snapshot. Verified on Docker Desktop (3 workers).
- Workers are not spread one per node yet (anti-affinity / topology spread).
- Git source fails where the tests volume is owned by root (`safe.directory` fix pending).

---

## 5. Platform (free, self-hosted)

| Part | Choice |
|---|---|
| Result storage | ✅ MinIO (S3-compatible), one bucket: `runs/` (results) and `test-plans/` (suites); expiry by a lifecycle rule (e.g. `runs/` after 90 days) |
| Live metrics | ✅ **Done (v1).** Each worker follows its own `simulation.log` while it is written (`LiveLogReader`) and serves Prometheus metrics (`--metrics-port` / `METRICS_PORT`; LoadTest `spec.metrics`), so the numbers equal the report's. Verified: counters grew during a run and ended at 264 = the report's 264; chunked-write test with 5 random cut patterns. Latency = Gatling's log chunk size / write rate (~1 s at high load, up to ~20 s at a few req/s). Grafana dashboard `docs/grafana/perf-dashboard.json` — **valid JSON, not yet tried in a real Grafana/Prometheus**. Note: the reader is tied to Gatling 3.13.1's log format, like the merger. |
| Server metrics | Existing Prometheus/APM alongside the Gatling dashboard |
| Load generators | Dedicated (spot) node pool that scales to zero between tests |

---

## 6. `perf-dashboard` — web UI

Design: [perf-dashboard-design.md](perf-dashboard-design.md). Flask + React, own Helm chart.

| Item | Status |
|---|---|
| Runs, Run detail (phases, live cards and charts, per-request tables, workers table, pod logs), reports from MinIO | ✅ v1 |
| New run form → LoadTest | ✅ v1 — from saved test plans only (since 2026-11-18) |
| History of run folders with final numbers | ✅ v1 |
| Test plans: suites in S3 — create, upload (files / folder / zip), edit, download, delete, Run | ✅ 2026-11-18 |
| Import LoadRunner script (VuGen folder / zip → new suite, conversion notes on screen; same converter as the CLI) | ✅ 2026-11-18 — verified on Docker Desktop (import, edit load, 2-worker run Passed) |
| Delete runs and their results (History; Run detail with "also delete results") | ✅ 2026-11-18 |
| Login (OIDC / SSO), audit of who started or deleted what | open |
| Run-to-run comparison, PVC results, plan edit history | open |

---

## Decisions

| # | Question | Decision |
|---|---|---|
| 1 | Kubernetes platform | **On-prem Kubernetes**, deployed with Argo CD (company) — verified on Docker Desktop Kubernetes and minikube |
| 2 | Result storage | **MinIO** (S3-compatible) |
| 3 | How test files reach pods | **Suites in MinIO, managed on the dashboard** (per-run copy); Git / ConfigMap / image kept for CI and GitOps |
| 4 | Oracle versions and features (stored procedures, REF CURSOR?) | open — needs a database to verify |
| 5 | Existing metrics stack | open — Prometheus endpoint per worker and a Grafana dashboard are ready, not yet tried |
