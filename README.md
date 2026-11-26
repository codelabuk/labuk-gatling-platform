# labuk-gatling

YAML-driven performance testing on **open-source Gatling** — a replacement for JMeter and LoadRunner. Test HTTP /
REST APIs (single calls, multi-API flows, polling), **SQL / PL-SQL** on any JDBC database, **Apache Arrow Flight**
servers, and **converted LoadRunner scripts** — on one machine, or **distributed across many workers on
Kubernetes**, with a web dashboard for testers.

**Testers write YAML, not code.** One generic engine reads a test plan and runs it; API names, variables and
conditions live in the YAML, the engine has no business logic in it. No Gatling Enterprise licence is needed:
distribution, merged results, live metrics and the UI are built here.

## Contents

- [What is in this repository](#what-is-in-this-repository) · [Who uses what](#who-uses-what) · [Getting started](#getting-started)
- Writing tests: [Organising tests](#organising-tests) · [Writing a plan](#writing-a-plan) · [API catalog](#api-catalog) ·
  [Steps](#steps) · [Data](#data) · [Authentication](#authentication) · [Load](#load) · [SLAs](#slas)
- Protocols: [SQL](#sql-load-testing-gatling-sql) · [Arrow Flight](#arrow-flight-load-testing-gatling-flight) ·
  [LoadRunner scripts](#loadrunner-scripts-perf-convert-lr)
- Running: [Distributed runs — reference](#distributed-runs--reference) · [On Kubernetes](#on-kubernetes-loadtest-operator) ·
  [perf-dashboard](#perf-dashboard-web-ui) · [The `perf` command](#the-perf-command-developers-and-ci)
- [LoadRunner / JMeter → this framework](#loadrunner--jmeter--this-framework) · [Troubleshooting](#troubleshooting) ·
  [For framework developers](#for-framework-developers) · [Further documents](#further-documents)

---

## What is in this repository

| Module | Status | What it is |
|---|---|---|
| `gatling-api-core` | ✅ v1.1 | The engine: YAML loader + validation, HTTP, auth, data, flow control, load profiles, SLAs, distributed-run support (worker split, synchronised start, setup once, log merge, run summaries), live metrics |
| `gatling-sql` | ✅ v1 | `sql:` steps and `jdbc` data sources — any JDBC database (Oracle and PostgreSQL drivers included) |
| `gatling-flight` | ✅ v1 | `flight:` steps and `flight` data sources — Apache Arrow Flight reads (GetFlightInfo + DoGet) and writes (DoPut); verified against labukDB |
| `gatling-runner` | ✅ v1 | The `perf` command (zip for Windows / Linux / macOS), the LoadRunner converter, the S3 client and the runner image every pod uses |
| `loadtest-operator` | ✅ v1 | Kubernetes operator (Java, JOSDK + fabric8): one `LoadTest` = validate → setup → N workers → merged report; spreads workers over nodes; Helm chart |
| `perf-dashboard` | ✅ v1 | Web UI for testers: test plans, LoadRunner import, new runs, live metrics per worker, final numbers, logs, reports, history, clean-up — on Kubernetes (with MinIO) or **on one machine without Kubernetes or Docker** (local mode); Helm chart |

Stack: Java 17, Maven 3.9, Scala 2.13.15, Gatling 3.13.1 (pinned), Arrow Flight 16.1, Java Operator SDK 5.1 on
fabric8 7.3, Flask + React. Target platform: on-prem Kubernetes with MinIO / S3.

---

## Who uses what

| Who | Uses | For |
|---|---|---|
| **Perf tester / QA** | **perf-dashboard** in the browser | Test plans (upload, edit, import LoadRunner scripts), start runs, watch them live, open reports, history — no tools to install |
| Framework developer | `perf` CLI, Maven | Debugging a plan locally, building images and charts, the LoadRunner converter from the command line |
| CI pipeline / GitOps | `perf submit` / `perf status`, or a `LoadTest` in Git (Argo CD) | Scheduled or merge-request runs; the exit code is the SLA verdict |
| Platform team | Helm charts | Install the operator and the dashboard; MinIO bucket and credentials |

---

## Getting started

**A. Testers, on Kubernetes** (the dashboard is installed — see [On Kubernetes](#on-kubernetes-loadtest-operator)):

1. Open the dashboard (e.g. `https://perf-dashboard.codelabuk.dev`).
2. **Test plans** → *New suite* (starter plan), *Upload folder / zip*, or *Import LoadRunner script*. Edit the YAML in
   the browser.
3. **New run** → choose the suite and plan, the number of **workers** (pods), memory per worker, and the variables
   (e.g. `BASE_URL=https://sit.api.example.com`) → *Start run*.
4. **Run detail** shows the phases, live numbers per worker and pod logs; *Open report* when it is done. **History**
   keeps every run.

**B. One machine, no Kubernetes / Docker** — the same dashboard with only Java 17+ and Python 3.10+ (each worker is
a process): from a checkout `cd perf-dashboard && start.cmd` (or `./start.sh`), or the
`perf-dashboard-local.zip` package on another machine. Open http://127.0.0.1:5000.
See [Without Kubernetes (local mode)](#without-kubernetes-local-mode).

**C. Developers, the `perf` command:**

```bash
mvn install -DskipTests                                     # builds everything; the zip: gatling-runner/target/perf-1.0-SNAPSHOT.zip
perf validate plans/orders.yaml --workers 4                 # check a plan, see the per-worker split
perf run plans/orders.yaml -DBASE_URL=https://sit.api.example.com
bash scripts/local-distributed-demo.sh                      # a distributed run (setup once, 2 workers, merged report) on one machine
```

The sample plans in [`gatling-api-core/src/test/resources/plans`](gatling-api-core/src/test/resources/plans) run
against a built-in mock API — the best place to start copying from.

---

## Organising tests

Keep tests in their own folder — no code in it. On Kubernetes the folder is usually a **suite** on the
dashboard's *Test plans* page (stored in `s3://perf-results/test-plans/<suite>/`; upload it as a zip or folder);
for CI / GitOps it can also be a Git repository:

```
perf-tests/
├── apis/          orders.yaml, payments.yaml    # API catalog: each API defined once
├── data/          users.csv, accounts.csv       # test data
├── bodies/        create-order.json             # request bodies (optional)
└── plans/         orders-peak.yaml, payments-soak.yaml
```

Paths resolve **relative to the file they are written in**: `include` and paths in the plan relative to the plan, body and CSV files in an included API catalog relative to that catalog — so a catalog works from any plan.

---

## Writing a plan

```yaml
name: order-flow
baseUrl: ${BASE_URL:https://sit.api.example.com}   # ${NAME:default} - override with -DBASE_URL=...
include: [ ../apis/orders.yaml ]                  # API catalog files

http:                                              # optional
  headers: { X-Client: perf-test }                 # sent on every request
  shareConnections: false                          # true = one connection pool for all users (service-to-service)

dataSources:
  users: { file: ../data/users.csv, strategy: queue }

auth: { type: login, api: login, token: $.token, refreshBefore: 60s }

setup: ...          # optional - runs once before the load
scenarios: [ ... ]  # required - what virtual users do, and how many
sla: [ ... ]        # optional - pass/fail criteria
maxDuration: 30m    # optional - hard stop
```

Unknown or misspelled keys are rejected with the file and exact location, e.g.
`orders.yaml: scenarios[0].steps[2].pollUntil: unknown key 'maxAtempts' - allowed: condition, maxAttempts, ...`

### Variables

- `#{name}` — a **runtime** value from the virtual user's session: a CSV column, something extracted from a
  response, a `forEach` item, a `set` value. Gatling EL functions also work: `#{randomUuid()}`,
  `#{currentTimeMillis()}`, `#{list.size()}`, `#{list.random()}`, `#{map.key}`.
- `${NAME}` / `${NAME:default}` — a **load-time** value from `-DNAME=...` or an environment variable.
  Use it for environments and secrets; never put passwords in YAML.

---

## API catalog

```yaml
apis:
  login:
    request: POST /auth/login                 # or:  method: POST  +  path: /auth/login
    body:
      json: { username: "#{username}", password: "#{password}" }
    checks:
      - status: 200

  getOrder:
    request: GET /orders/#{orderId}
    headers: { X-Correlation-Id: "#{randomUuid()}" }
    query: { expand: items }
    checks:
      - status: [200, 304]
      - jsonPath: $.id
        equals: "#{orderId}"
    extract:                                   # saved into the user's session
      orderStatus: $.status                    # short form = jsonPath, string
      itemIds: { jsonPath: "$.items[*].id", all: true }
```

**Body** — one of:
`json:` (inline YAML or a JSON string), `file: ../bodies/x.json`, `raw: "<xml/>"` + `contentType:`,
`form: { field: value }`. All may contain `#{...}`.

**Checks** — a failed check marks the request KO in the report:

| Check | Example |
|---|---|
| `status` | `status: 200` or `status: [200, 201]` (default when no status check: 2xx/304) |
| `jsonPath` | `jsonPath: $.id` (exists) · `equals: "x"` · `notExists: true` |
| `regex` / `header` | `regex: 'id="(\d+)"'` · `header: Location` (+ `equals` / `notExists`) |
| `bodyContains` | `bodyContains: SUCCESS` (+ `notExists: true`) |

**Extract** — `name: $.path` or `name: { jsonPath | regex | header: ..., all: true, optional: true, type: ... }`.
`type` (jsonPath only): `string` (default), `int`, `long`, `number`, `boolean`, `object`, `list`, `any`.
Without `optional: true`, a missing value fails the request.

A step can add `checks` / `extract` on top of the catalog's; a step extract with the same name replaces the
catalog one.

---

## Steps

| Step | Example | Notes |
|---|---|---|
| `api` | `- api: getOrder` | Optional `name:` (report label), `extract:`, `checks:` |
| `transaction` | `- transaction: checkout`<br>`  steps: [ ... ]` | Timed group in the report (LoadRunner transaction) |
| `wait` / `thinkTime` | `- wait: 2s` · `- wait: 1s..3s` · `- wait: { duration: "#{waitSeconds}s", max: 5m }` | Can come from a response; `max` caps it |
| `pace` | `- pace: 10s` | Inside loops: each iteration takes at least this long (LoadRunner pacing) |
| `pollUntil` | see below | Repeat steps until a condition holds; KO if it never does |
| `forEach` | `- forEach: { list: itemIds, as: itemId, steps: [ ... ] }` | Loop over an extracted list |
| `repeat` | `- repeat: { times: 5, steps: [ ... ] }` | `times` may be `"#{n}"` |
| `loop` | `- loop: { during: 10m, steps: [ ... ] }` | Keep iterating for a duration (closed-model soak tests) |
| `if` | `- if: { condition: "#{status} == 'OPEN'", then: [ ... ], else: [ ... ] }` | |
| `randomSwitch` | `- randomSwitch: [ { percent: 70, steps: [...] }, { percent: 30, steps: [...] } ]` | Traffic mix |
| `retry` | `- retry: { times: 3, steps: [ ... ] }` | Retries the block if anything in it fails |
| `rendezvous` | `- rendezvous: 50` | Users wait until 50 arrive, then go together (per worker when distributed) |
| `set` | `- set: { channel: WEB, runId: "#{randomUuid()}" }` | |
| `feed` | `- feed: accounts` | Take the next record mid-flow |
| `log` | `- log: "order #{orderId} is #{orderStatus}"` | Printed to the console |
| `stopUserIfFailed` | `- stopUserIfFailed: true` | End this virtual user if anything failed so far |

### pollUntil

```yaml
- pollUntil:
    condition: "#{status} != 'PENDING'"
    maxAttempts: 10        # at least one of maxAttempts / timeout is required
    timeout: 15m
    failMessage: request never left PENDING      # optional - shown in the report's Errors table
    name: time-to-complete                       # optional - see below
    steps:
      - api: getWaitTime
        extract: { waitSeconds: $.waitSeconds }
      - wait: { duration: "#{waitSeconds}s", max: 5m }
      - api: getStatus
        extract: { status: $.status }
```

The condition is checked **before** each attempt, so extract the first value before the poll.
If it is still false after `maxAttempts`/`timeout`, the report gets a KO entry (named `pollUntil`, or the
poll's `name`) with the `failMessage` in its Errors table, and the enclosing transaction turns KO.
With a `name`, every poll also records an OK entry whose response time is **how long the condition took to
become true** — usable in SLAs: `api.time-to-complete.responseTime.p95 < 2m`.
Waiting users cost almost nothing — 50k users waiting a minute do not tie up threads.

### Conditions

Used by `pollUntil` and `if`:

```
#{status} != 'PENDING'
#{retries} < 5 and not #{done}
exists #{token}
#{roles} contains 'admin' or (#{code} == 200 && #{items.size()} > 0)
```

Operators: `==  !=  <  <=  >  >=  contains`, `and`/`&&`, `or`/`||`, `not`/`!`, `exists`, parentheses.
Numbers compare numerically, everything else as text. A bare `#{flag}` is true unless missing, empty,
`false` or `0`.

### Durations

`500ms`, `2s`, `1.5s`, `10m`, `1h`, `1m30s`, `30` (= seconds), ranges `1s..3s` (random in between).

---

## Data

```yaml
dataSources:
  users:    { file: ../data/users.csv, strategy: queue }
  products: { file: ../data/products.csv, strategy: random, separator: ";" }
```

CSV with a header row; columns become `#{column}` variables.

| `strategy` | Behaviour |
|---|---|
| `circular` (default) | In order, wrapping around |
| `queue` | Each record once — when it runs out, further users stop (a warning is printed up front if the file is too small for the planned load) |
| `random` | Random record each time |
| `shuffle` | Shuffled once, then like `queue` |

`splitAcrossWorkers: true` (default) gives each worker a different slice in distributed runs, so e.g. no
login is used by two pods.

A scenario takes one record from each feeder at its start: `feeders: [ users, products ]`.

### Setup and shared lists

`setup` runs **once** before the scenarios start — e.g. fetch a list that virtual users then work through:

```yaml
setup:
  vars: { username: ${SETUP_USER}, password: ${SETUP_PASSWORD} }   # account used for setup
  steps:
    - api: loadFilterData
      extract:
        srList: { jsonPath: "$.items[*].srId", all: true }
  share:
    srQueue: { from: srList, as: sr }      # each user gets one item as #{sr}

scenarios:
  - name: process-items
    feeders: [ users, srQueue ]
```

If list items are JSON objects (`type: object`), their fields become variables directly. `strategy`
defaults to `queue` for shares. If setup fails, the test stops immediately — it cannot run without its data.

---

## Authentication

Set once at plan level (applies to setup and every scenario); a scenario can override it or use
`auth: none`. The token is added to every request automatically (except the login call) — catalog APIs do
not need an `Authorization` header.

```yaml
# 1. call a login API (credentials from the users CSV) and refresh before the JWT expires
auth: { type: login, api: login, token: $.token, refreshBefore: 60s }
#    optional: saveAs: token, header: Authorization, prefix: "Bearer ", ttl: 15m (if the token is not a JWT)

# 2. mint a signed JWT locally (environments that trust a test secret) - no login traffic
auth:
  type: jwt
  secret: ${JWT_SECRET}
  secretEncoding: text          # or base64
  algorithm: HS256              # HS384, HS512
  claims: { sub: "#{username}", roles: tester }
  validity: 1h

# 3. a fixed token / API key
auth: { type: static, value: ${API_KEY}, header: X-Api-Key, prefix: "" }
```

---

## Load

```yaml
scenarios:
  - name: browse
    load:
      profile:
        - rampUsersPerSec: { from: 1, to: 100, during: 5m }
        - constantUsersPerSec: { rate: 100, during: 30m, randomized: true }
    throttle:                   # optional cap on requests/sec (LoadRunner goal-oriented)
      - reachRps: 500
        in: 1m
      - holdFor: 30m
```

**Open model** — new users arrive at a rate (APIs, websites): `atOnceUsers`, `rampUsers {users, during}`,
`constantUsersPerSec {rate, during}`, `rampUsersPerSec {from, to, during}`, `stressPeakUsers {users, during}`,
`incrementUsersPerSec {by, times, eachLevelLasting, separatedByRampsLasting, startingFrom}`, `nothingFor`.
**Closed model** — a fixed number of concurrent users (call centres, soak tests with `loop`):
`constantConcurrentUsers {users, during}`, `rampConcurrentUsers {from, to, during}`,
`incrementConcurrentUsers {by, times, eachLevelLasting, separatedByRampsLasting, startingFrom}`.
A profile cannot mix the two. Short forms also work: `- constantConcurrentUsers: 200` + `during: 10m`.

**Rates** take units: `5` or `5/s`, `266/min`, `16000/h`. A rate counts **virtual users starting the
scenario**, not HTTP requests — a scenario with 3 API calls at `100/min` sends ~300 requests/min.

In distributed runs all numbers are **totals** — each worker runs its share automatically.

### From JMeter: "266 requests/min, ~16k in an hour, load increased in steps"

```yaml
# one request per virtual user -> rate = requests
load:
  profile:
    - incrementUsersPerSec: { by: 66/min, times: 3, eachLevelLasting: 5m, separatedByRampsLasting: 1m, startingFrom: 66/min }
    - constantUsersPerSec: { rate: 266/min, during: 1h }       # ~15,960 requests
```

The same as JMeter's "N threads + Constant Throughput Timer" (closed model + pacing):

```yaml
steps:
  - loop:
      during: 1h
      steps:
        - pace: 4.5s                 # each user starts an iteration at most every 4.5s
        - api: getOrder
load:
  profile:
    - constantConcurrentUsers: { users: 20, during: 1h }    # 20 users / 4.5s = ~266/min
```

`throttle` can also cap the total rate, but only in whole requests/second per worker. **Gatling stops the scenario when the throttle schedule ends** — users still waiting for the cap are dropped — so make `holdFor` cover the whole test; `perf validate` and the run warn when it does not.

### Sizing a load generator

One machine handles thousands of concurrent users — measured on a 4-CPU, 16 GB laptop with `-Dperf.heap=4g`:
**5,000 concurrent users, 62k requests at ~650 req/s, 0 errors, p95 7 ms** (against a mock on the same
machine). Rough guide:

- Memory grows with concurrent users and response sizes; start with `-Dperf.heap=4g` on an 8 GB machine.
- Watch the load generator's CPU: above ~70-80% its own timings get less accurate — add a worker instead.
- Every virtual user opens its own connections (like real users), so behind a load balancer the traffic
  spreads across nodes the way real traffic does; `JSESSIONID` and other cookies are kept per user, so
  sticky sessions behave normally. Set `http.shareConnections: true` only for service-to-service APIs.
- Windows limits outgoing ports (~16k by default); very high new-connection rates from one machine can run
  out — use more workers or longer keep-alive.
- Bigger than one machine: [Sizing a distributed test](#sizing-a-distributed-test).

---

## SLAs

```yaml
sla:
  - global.failedRequests.percent < 1
  - global.responseTime.p95 < 800ms
  - api.getOrder.responseTime.p99 < 1.5s
  - transaction.checkout.failed.percent < 2
  - global.requestsPerSec >= 200
```

`<scope>.<metric> <op> <value>` — scope `global`, `api.<name>`, `transaction.<name>`;
metrics `responseTime.{min,max,mean,stdDev,p50,p75,p90,p95,p99,p999}`,
`{failed,successful,all}Requests.{percent,count}` (short: `failed.percent`), `requestsPerSec`;
ops `< <= > >= ==`. Results print at the end of the run, and a failed SLA fails the build — use it in CI.

---

## SQL load testing (`gatling-sql`)

Plans that use `sql:` steps run from the **`gatling-sql`** module (it contains the core, the SQL support and
the Oracle + PostgreSQL drivers); everything else in this README works there too:

```bash
mvn -pl gatling-sql test-compile gatling:test -DskipTests -Dplan=plans/orders-db.yaml -DDB_PASSWORD=...
# demo: database load + API-then-database flow, against in-memory Derby and the mock API
mvn -pl gatling-sql test-compile gatling:test -Dgatling.simulationClass=org.codelabuk.perf.sql.MockedSqlSimulation
```

### Connections and DB test data

```yaml
dataSources:
  ordersDb:
    type: jdbc
    url: ${DB_URL}                     # jdbc:oracle:thin:@//host:1521/SERVICE, jdbc:postgresql://host/db, ...
    user: ${DB_USER}
    password: ${DB_PASSWORD}
    pool: { size: 20, connectionTimeout: 10s }   # all connections are opened before the load starts
    allowWrites: false                 # default: only SELECTs; set true for update/call steps
    maxSessions: 200                   # optional: warns if pool size x workers would exceed it
    fetchSize: 500                     # optional
    properties: { oracle.jdbc.ReadTimeout: "60000" }   # optional driver properties
    # driver: oracle.jdbc.OracleDriver  # optional - normally detected from the URL

  activeCustomers:                     # rows of a query as test data (one row per virtual user)
    type: jdbc
    datasource: ordersDb
    query: "SELECT customer_id, account_no FROM customers WHERE status = ? ORDER BY customer_id"
    params: [ ACTIVE ]
    strategy: queue                    # same strategies as CSV; split across workers by default
```

An unreachable database stops the run at start-up with `Cannot connect to jdbc connection '...'`.
Column names are lower-case in variables (`#{customer_id}`), whatever the database returns.

### The `sql` step

```yaml
- sql:
    datasource: ordersDb
    name: find-orders                  # label in the report (default "sql <datasource>") - usable in SLAs
    query: "SELECT id, status FROM orders WHERE customer_id = ?"      # or update: / call:
    params: [ "#{customer_id}" ]
    checks:
      - rowCount: ">= 1"
      - column: status
        equals: OPEN
    extract:
      orderId: id                      # first row, column id
      allIds: { column: id, all: true }
      orderRows: { rows: true }        # all rows as a list (use with forEach / setup share)
      found: { count: true }           # number of rows (or rows updated)
    timeout: 5s                        # optional query timeout
    maxRows: 1000                      # default 1000
```

| Statement | Use | Notes |
|---|---|---|
| `query:` | `SELECT` / `WITH` | Read-only; checks `rowCount`, `column` + `equals` |
| `update:` | `INSERT` / `UPDATE` / `DELETE` / DDL | Needs `allowWrites: true`; check `updateCount` |
| `call:` | Stored procedures `{call pkg.proc(?, ?)}` | Needs `allowWrites: true`; OUT params, result rows |
| `batch:` | Many rows in one round-trip | Needs `allowWrites: true`; `rows:` is a YAML list (`- [ "#{sr}", note ]`) or a variable (`rows: "#{orderIds}"`; for a list of maps add `columns: [id, status]`); check `updateCount` (the total) |

**Parameters**, one per `?`: a literal (`42`, `ACTIVE`), a variable (`"#{orderId}"`), a typed value
`{ value: "19.99", type: decimal }` (types: `string int long decimal double boolean date timestamp null`), or
for procedures an OUT parameter `{ out: int, saveAs: newId }` (out types add `number` and `cursor` for
Oracle REF CURSOR, saved as a list of rows).

**`rollback: true`** runs a write and rolls it back: the timing is real, nothing is kept — useful against
shared test data.

**`reportConnectionWait: true`** adds a `<name> (connection)` entry timing how long the step waited for a
free connection. If that is high, the pool is too small; if the statement itself is slow, it is the database.

How it runs: JDBC calls block, so each connection pool has its own threads (pool size = threads); Gatling's
own threads are never blocked. The measured time includes waiting for a free connection, as a real
application would experience it.

Other databases: add the driver as a dependency in `gatling-sql/pom.xml` (SQL Server, MySQL, DB2, ...) —
no code change.

---

## Arrow Flight load testing (`gatling-flight`)

`flight` steps call an **Apache Arrow Flight** server (gRPC): reads with `GetFlightInfo` + `DoGet`, writes with
`DoPut`. Built for and verified against [labukDB](https://github.com/codelabuk/labukDB)'s Flight server; any Flight
server whose commands are bytes (JSON, SQL text, …) works the same way.

```yaml
dataSources:
  labukdb:
    type: flight
    uri: ${FLIGHT_URI:grpc://localhost:9443}   # grpc://, grpc+tls://, grpc+unix://
    pool: { size: 16, connections: 4 }         # call threads (calls in flight at once), gRPC channels
    timeout: 60s                               # per call
    # headers: { authorization: "Bearer ${TOKEN}" }   verifyServer: false (self-signed TLS)

scenarios:
  - name: readers
    steps:
      - flight:
          datasource: labukdb
          name: scan-level
          query:                                # the descriptor's command - a YAML object is sent as JSON
            table: logs
            predicate: { type: cmp, column: level, op: EQ, literal: "#{level}", literalType: string }
          fetch: all                            # all | first | none (plan only) endpoints
          parallel: 3                           # endpoints fetched at once
          phases: true                          # also "scan-level (plan)" and "scan-level (first batch)"
          checks: [ { endpoints: ">= 1" }, { rows: ">= 1" } ]     # rows | endpoints | batches | bytes
          extract: { first: { column: amount }, all: { column: service, all: true }, n: { count: true } }

  - name: writers
    steps:
      - flight:
          datasource: labukdb
          name: put-warn
          put: { table: logs, partitionValues: { level: WARN } }   # the DoPut command
          rows: [ { service: perf, amount: 1, event_time: "2026-11-22T12:00:00Z", id: "#{randomUuid()}" } ]
          times: 50                             # repeat the rows (re-resolved: a new UUID each time)
          batchRows: 25                         # rows per Arrow batch
          schema: { service: string, amount: long, event_time: timestampNtz, id: string }   # optional, see below
          checks: [ { rows: "== 50" }, { batches: "== 2" } ]
```

| | |
|---|---|
| Timings | The step is one request in the report (plan + all fetches, or the whole put until the server acknowledges). `phases: true` adds `(plan)` = GetFlightInfo and `(first batch)` = time to the first Arrow batch. SLAs work as for APIs: `api.scan-level.responseTime.p95 < 500ms` |
| Endpoints | One `DoGet` per endpoint (labukDB: one per data file, or one combined endpoint for `groupBy` / `orderBy` / `limit`). Endpoints on another server are fetched from that server |
| Writes | Rows come from the YAML (values may be `#{...}`) or a session variable holding a list of maps (`rows: "#{myRows}"`). Without `schema:`, the column types come from the server's `GetSchema` (by default for the put's `table` / `tableRoot`, or `schemaFrom:`), minus the `partitionValues` columns. Types: `string`, `long`, `int`, `short`, `byte`, `double`, `float`, `boolean`, `timestamp` (UTC), `timestampNtz` (no time zone), `date`; timestamps accept ISO-8601 text or epoch millis |
| Threads | Flight's Java client blocks, so calls run on the data source's own threads (`pool.size`), never on Gatling's — HTTP timings in the same test stay accurate. `pool.size` limits how many calls are in flight at once |
| Big results | Decoding Arrow costs CPU and memory on the load generator: for very large scans use `fetch: first`, `maxRows` (rows kept for extraction, default 1000) and enough workers |

**labukDB notes** (found while testing it): `GetSchema` reports timestamp columns as `Int64`, while its Delta writer
needs a timestamp **without** time zone — so for writes to tables with timestamps give `schema:` with
`timestampNtz`. `DoPut` commits once per call (one new data file per put), so reads of that partition fetch more
endpoints as writers run. Example plan: `gatling-flight/src/test/resources/plans/labukdb.yaml`.

```bash
FLIGHT_URI=grpc://localhost:9443 LABUKDB_TABLE=/data/perf_logs perf run plans/labukdb.yaml
```

The runner needs `--add-opens=java.base/java.nio=ALL-UNNAMED` for Arrow's off-heap memory; the `perf` launchers,
the Dockerfile and the Jib image set it.

---

## LoadRunner scripts (`perf convert-lr`)

**Testers:** dashboard → *Test plans* → **Import LoadRunner script**: choose the VuGen folder or a zip of it, give
a new suite name, *Convert and save*. The suite gets the plan, API catalog, data and the conversion report; the
notes are shown on screen. Set the load in the plan (editor), then *Run it*. Recording snapshots and other
non-script files in the folder are ignored. The dashboard image contains Java and the perf distribution (copied
from the runner image), so this is exactly the CLI's conversion.

**Developers / CI:**

```bash
perf convert-lr path/to/VuGenScript --out perf-tests/sr-flow     # [--iterations N]
```

Reads `vuser_init.c`, `Action*.c`, `vuser_end.c` and the `.prm` / `.dat` parameter files and writes a YAML plan,
an API catalog, the CSV data and `<script>-conversion.txt` — **everything not converted is listed there with
file and line, never silently dropped.** Then set `load` (a VuGen script has none — it lives in the Controller
scenario), `perf validate` and run it like any plan.

| VuGen | YAML |
|---|---|
| `web_url`, `web_custom_request`, `web_submit_data` | API catalog + `api` steps (method, URL, JSON / raw / form body); the most used host becomes `baseUrl` |
| `web_reg_save_param` (LB/RB, `Ord=ALL`), `_ex`, `_json`, `_regexp` | `extract` on the next request (regex / jsonPath / `all: true`) |
| `web_reg_find("Text=...")`, `Fail=Found` | `bodyContains` check / `notExists` |
| `web_add_header`, `web_add_auto_header`, `web_revert_auto_header` | headers on the next / all following requests |
| `lr_start_transaction` / `lr_end_transaction` (nested) | `transaction` blocks |
| `lr_think_time`, `lr_rendezvous`, `lr_save_string`, `lr_output_message` | `wait`, `rendezvous` (placeholder 1 - set it), `set`, `log` |
| `{Param}` from `.prm` File/Table parameters | `#{column}` + CSV data source (Sequential → circular, Random → random, Unique → queue) |
| `vuser_init` / `Action` / `vuser_end` | steps once / in `repeat` (`--iterations`) / once |
| `if` / `for`, `lr_eval_string`, `web_link`, other parameter types, unknown calls | listed in the report |

Verified with a recorded-style script (`gatling-runner/src/test/resources/lr/SR_Flow`): the converted plan runs
in Gatling with every request and transaction OK, and ran distributed on Kubernetes (3 workers, Passed). Through
the dashboard: imported as a zip, load set to 6 users in the editor, run on 2 workers — Completed / Passed,
24 requests (12 + 12), 0 KO, the three LoadRunner transactions in the report.
It is not a comparison against LoadRunner's own numbers - for that, run the same scenario in both tools.

---

## Distributed runs — reference

One machine runs thousands of virtual users; bigger tests run **the same plan on several workers at once** — pods on
Kubernetes, or processes on one machine. Nothing in the plan changes: every load number in it is a **total**.

```
            ┌────────────┐   ┌─────────────┐   ┌────────── workers (in parallel) ─────────┐   ┌──────────────┐
 plan ───►  │  validate  │─► │ setup (once)│─► │ worker 0 │ worker 1 │ ... │ worker N-1     │─► │ report       │
            │ (split ok?)│   │ shared lists│   │ all start at START_AT, each 1/N of the load │   │ merge → one  │
            └────────────┘   └─────────────┘   └─────────────────────────────────────────────┘   │ report + SLAs│
                                                                                                └──────────────┘
```

### How the load and the data are split

| What | How it is split | Largest difference between workers |
|---|---|---|
| Users (`atOnceUsers`, `rampUsers`, `constantConcurrentUsers`, …) | `total / N`; the remainder goes to the first workers | 1 user |
| Rates (`constantUsersPerSec`, `rampUsersPerSec`, …) | divided exactly (5/s on 3 workers = 1.67/s each) | none |
| `throttle` targets | divided, whole requests/s per worker (minimum 1 each — a warning is printed) | rounding |
| CSV rows, `jdbc` feeder rows, setup's shared lists | **round-robin**: worker *i* takes rows *i*, *i+N*, *i+2N*, … — no two workers use the same login / item, and a sorted list cannot put all "expensive" items on one worker | 1 row |
| `rendezvous: 50` | waits for 50 users **per worker** | — |

`splitAcrossWorkers: false` on a data source gives every worker all its rows. Shared lists are sorted before splitting,
so every worker splits an identical list.

**Not like Spark — no rebalancing at runtime, on purpose.** A load test must apply the planned load shape (this many
users, at this rate, from the start time) rather than finish a fixed amount of work quickly. Workers can still differ
in *requests* because items cost different amounts (e.g. a `pollUntil` that loops longer for some items); the merged
report counts every request. Watch for: fewer users than workers (some workers idle), too little `queue` data (users
stop), a worker that starts late (image pull longer than the start delay), several workers on one node.
`perf validate --workers N` prints the split per worker before you run; the dashboard's workers table shows
requests, users and p95 per worker while it runs.

### One result per run

1. Each worker writes its Gatling log (`worker-<i>.log`) and its own final numbers (`worker-<i>.json`).
2. The report step **merges all logs into one** (records re-based to a common start; tied to Gatling 3.13.1's log
   format) and builds **one HTML report**; the **SLAs are judged on all the data**. A worker's own SLA printout covers
   its slice only and is informational.
3. `report/summary.json` holds the run's final numbers, computed exactly like the HTML report (same t-digest
   percentiles), for the dashboard and History.

While it runs, every worker serves live Prometheus metrics; the dashboard sums them (and Prometheus can scrape them).

### Ways to run a distributed test

| Way | Who | How |
|---|---|---|
| **perf-dashboard on Kubernetes** | testers | *New run* → *Workers (pods)* = N; tests from a saved suite |
| **perf-dashboard, local mode** | one machine | *Workers (processes)* = N (up to `LOCAL_MAX_WORKERS`, default 8) — shares one machine's CPU |
| **`perf submit`** | developers, CI | creates the `LoadTest` for you; `perf status --watch` exits with the verdict |
| **A `LoadTest` resource** | `kubectl apply`, GitOps (Argo CD) | the YAML below |
| **By hand** | any machines | `perf setup`, N × `perf worker`, `perf report` — see [By hand](#by-hand-without-the-operator) |

### Phases

| Phase | Kubernetes (Job) | Local mode (process) | What happens |
|---|---|---|---|
| Validating | `<run>-validate` | `perf validate` | The plan is checked for N workers; a bad plan stops here with the exact YAML error in the run's message. Tests from a saved suite are copied into `<run folder>/tests` first |
| Setup | `<run>-setup` | `perf setup` | Only if the plan has `setup.share`; runs **once**, writes `shared.json` |
| Starting → Running | `<run>-workers` (Indexed Job, N pods) | N × `perf worker` | All wait for `START_AT` (= now + start delay), then each runs its share; no retries — a crashed worker fails the run |
| Aggregating | `<run>-report` | `perf report` | Merge, HTML report, SLA verdict, `summary.json` |
| Completed / Failed | — | — | `result` = `Passed` / `SLAFailed`, or the reason for the failure |

### The run folder

S3 / MinIO (`results.s3`), a shared volume (`results.pvc`) or, in local mode, the data folder — always the same layout:

```
perf-results/runs/<run>-<uid8>/
├── tests/                      the suite as it was when the run started (runs from saved test plans)
├── shared.json                 setup's lists (plans with setup)
├── logs/worker-<i>.log         each worker's Gatling log
├── logs/worker-<i>.json        each worker's final numbers
└── report/
    ├── summary.json            the run's final numbers + SLA verdict
    └── merged/index.html …     the merged Gatling HTML report
```

`perf fetch s3://perf-results/runs/<run>/report <dir>` downloads a report; the dashboard opens it in the browser.

### The `LoadTest` resource

```yaml
apiVersion: perf.codelabuk.org/v1alpha1
kind: LoadTest
metadata: { name: orders-peak-20261120, namespace: perf }
spec:
  image: registry.example.com/perf/labuk-perf-runner:1.0.0
  plan:
    file: plans/orders-peak.yaml
    s3: s3://perf-results/test-plans/orders-api          # a saved suite (the dashboard's Test plans)
  workers: 10
  env: { BASE_URL: https://sit.api.example.com }
  secretRefs: [ perf-secrets ]                          # passwords / tokens as ${...} values
  resources: { requests: { cpu: "2", memory: 4Gi }, limits: { cpu: "2", memory: 4Gi } }
  spread: soft
  results:
    s3: { url: s3://perf-results/runs, endpoint: http://minio.minio.svc:9000, credentialsSecret: minio-creds }
  metrics: { enabled: true }
  startDelaySeconds: 60
```

| `spec` field | Default | Meaning |
|---|---|---|
| `image` | — (required) | The runner image (`labuk-perf-runner`) |
| `plan.file` | — (required) | The plan: relative to the suite / repository / ConfigMap root, or an absolute path inside the image |
| `plan.s3` | — | Tests from S3 / MinIO (`s3://bucket/folder`); uses `results.s3`'s endpoint and credentials; copied into the run folder by the validate pod |
| `plan.git` | — | `{ repo, ref: main, tokenSecret: { name, key: token }, image: alpine/git:2.47.2 }` — `ref` may be a branch, tag or commit SHA |
| `plan.configMap` | — | A ConfigMap as the tests folder (flat, 1 MB) — what `perf submit --from-dir` creates |
| `plan.inImage` | `false` | Tests baked into the image (`file` absolute) |
| `workers` | `1` | Number of worker pods; all plan numbers are totals split across them |
| `scenarios` | all | Run only these scenarios |
| `env` | — | Environment variables for every pod (fill `${NAME}` in the plan) |
| `secretRefs` | — | Secrets whose keys become environment variables |
| `resources` | none | CPU / memory per worker; the JVM heap is 75 % of the memory limit. Requests must fit the nodes' free capacity |
| `nodeSelector`, `tolerations` | — | Put workers on dedicated load-generator nodes |
| `spread` | `soft` | `soft` = one worker per node where possible, never blocks; `hard` = at most one per node (extra workers stay Pending); `none` |
| `reportResources` | none | CPU / memory of the report pod (merge + report) — more memory for large runs |
| `results.s3` | — | `{ url: s3://bucket/prefix, endpoint, region: us-east-1, credentialsSecret }` (Secret with `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) |
| `results.pvc` | — | An existing claim instead of S3 (ReadWriteMany on multi-node clusters); exactly one of `s3` / `pvc` |
| `startDelaySeconds` | `60` | Time from creating the workers to their common start — must cover image pulls |
| `timeoutMinutes` | `180` | Hard limit for the worker phase |
| `ttlSecondsAfterFinished` | `3600` | Finished Jobs and pods (and their logs) are removed after this |
| `metrics` | `{ enabled: false, port: 9464, lingerSeconds: 30 }` | Live Prometheus metrics on every worker (+ `prometheus.io/*` annotations); runs from the dashboard enable it |

`status`: `phase`, `message`, `result` (`Passed` / `SLAFailed`), `slaPassed`, `plan`, `scenarios`, `hasSetup`, `runDir`,
`reportPath`, `startAt`, `startedAt`, `finishedAt`, `workersActive` / `workersSucceeded` / `workersFailed`.
A finished LoadTest is never re-run — create a new one (new name) for every run. Deleting it stops and removes its
Jobs and pods; its results stay in the run folder.

### Sizing a distributed test

- **Workers = target requests/s ÷ what one worker sustains below ~70 % CPU, plus ~30 % margin.** A virtual user is a
  small object, not a thread — waiting users (think time, polling) cost almost nothing, so the limit is requests per
  second, not users. Rough guide per worker: 2 CPU / 4 Gi ≈ 3–5k users with think time, ~1–2k req/s for small JSON
  responses (estimates — measure your cluster with `gatling-api-core/src/test/resources/plans/capacity.yaml`, a
  stepped-rate test for one worker, against a target that can take more than the worker sends).
- Put workers on **different nodes** (`spread: soft`, the default; `hard` to insist) — several workers on one node
  share its CPU, network and ports.
- Large runs (millions of requests) need memory in the report step: `reportResources`.
- `queue` data needs at least as many rows as users (in total; the split is automatic).
- The system under test is usually the real limit — ramp up gradually and agree the test with the people who run it.

### By hand (without the operator)

The same steps on any machines (one or many) with the `perf` command; `scripts/local-distributed-demo.sh` does this
on one machine against the mock API.

```bash
perf validate plans/orders.yaml --workers 2
perf setup    plans/orders.yaml --out shared/shared.json                 # only if the plan has setup.share
# on each worker machine / process (same START_AT everywhere):
PLAN=plans/orders.yaml WORKER_INDEX=0 WORKER_COUNT=2 START_AT=2026-11-20T10:00:00Z \
  SHARED_IN=shared/shared.json RESULTS_DIR=results/logs perf worker
PLAN=plans/orders.yaml WORKER_INDEX=1 WORKER_COUNT=2 START_AT=2026-11-20T10:00:00Z \
  SHARED_IN=shared/shared.json RESULTS_DIR=results/logs perf worker
perf report --input results/logs --output results/report                # one report + SLA verdict (exit 0 / 2)
```

| Worker setting (env / `-D`) | Meaning |
|---|---|
| `PLAN` | The plan |
| `WORKER_INDEX` (or `JOB_COMPLETION_INDEX`) / `WORKER_COUNT` | Which slice this worker runs, of how many |
| `START_AT` | Common start: ISO-8601 or epoch millis |
| `SHARED_IN` | setup's `shared.json` (without it each worker runs `setup` itself — it works, but calls the setup APIs N times) |
| `RESULTS_DIR` | Where `worker-<i>.log` / `.json` go — a shared folder or `s3://bucket/prefix/logs` |
| `SCENARIO`, `METRICS_PORT`, `METRICS_LINGER_SECONDS` | Scenario filter; live metrics port; how long final metrics stay up |

Every path also takes `s3://…` (credentials from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
`S3_ENDPOINT`). With Maven the same settings are `-Dworker.index`, `-Dworker.count`, `-Dstart.at`, `-Dmode=setup
-Dshared.out=…`, `-Dshared.in=…`.

### Good to know

- The start time is fixed when the workers are created (now + start delay); a worker whose image pull takes longer
  starts late (it logs a warning) — keep the image on the nodes or raise `startDelaySeconds`.
- A worker that cannot be scheduled (e.g. `Insufficient cpu`, or `spread: hard` with too few nodes) keeps the run
  in *Running* until `timeoutMinutes`; `kubectl describe pod` shows why.
- Log merging and live metrics are tied to the pinned Gatling 3.13.1 log format — re-check them when upgrading.
- Tests from Git: on clusters where the tests volume is owned by root while git runs as uid 1000 the clone fails
  (`detected dubious ownership`, seen on Docker Desktop) — use saved test plans (`plan.s3`) there.

---

## On Kubernetes (`loadtest-operator`)

The operator runs every `LoadTest` as Jobs (see [Distributed runs — reference](#distributed-runs--reference)).
Install it once with **Helm**:

```bash
mvn install                                                     # builds everything; refreshes the chart's CRD
docker build -t registry.example.com/perf/labuk-perf-runner:1.0.0 gatling-runner          # or: mvn -pl gatling-runner jib:build -Dimage=...
docker build -t registry.example.com/perf/labuk-loadtest-operator:1.0.0 loadtest-operator
helm upgrade --install loadtest-operator charts/loadtest-operator -n perf --create-namespace \
  --set image.repository=registry.example.com/perf/labuk-loadtest-operator --set image.tag=1.0.0
```

Chart values (`charts/loadtest-operator/values.yaml`): `image.*`, `imagePullSecrets`, `replicaCount` (2 = a standby,
leader election through a Lease), `resources`, `logLevel`, `nodeSelector` / `tolerations` / `affinity`,
`serviceAccount.*`, `rbac.create`, pod / container security contexts (non-root, read-only root filesystem). Helm
installs the CRD on first install but never upgrades it — after an upgrade that changes the CRD run
`kubectl apply -f charts/loadtest-operator/crds/`. Plain manifests (no Helm): `loadtest-operator/deploy/operator.yaml`.

**Start runs** from the dashboard, with `kubectl apply -f <LoadTest>.yaml`, or with `perf` (developers / CI — needs
`kubectl` access):

```bash
export PERF_IMAGE=registry.example.com/perf/labuk-perf-runner:1.0.0 PERF_NAMESPACE=perf
perf submit plans/orders-peak.yaml --workers 5 --git-repo https://git.example.com/qa/perf-tests.git \
     --git-ref main --env BASE_URL=https://sit.api.example.com --secret perf-secrets --memory 4Gi \
     --s3 s3://perf-results/runs --s3-secret minio-creds --s3-endpoint http://minio.minio.svc:9000
perf submit plans/sr.yaml --from-dir ./perf-tests --workers 2      # local folder -> ConfigMap (flat, 1 MB)
perf submit ... --dry-run                                           # print the LoadTest instead
perf status orders-peak-20261120-100000 --watch                     # exit 0 Passed, 2 SLAFailed, 1 Failed
```

```bash
kubectl get loadtests -n perf -w     # NAME  PHASE  RESULT  WORKERS  DONE  AGE   (-o wide adds FAILED, MESSAGE)
kubectl get lt orders-peak-20261120 -n perf -o jsonpath='{.status.result} {.status.reportPath} {.status.message}'
```

**End-to-end scripts:** `bash scripts/docker-desktop-e2e.sh` (Docker Desktop Kubernetes + MinIO: operator with 2
replicas, a 3-worker run, `perf status --watch`, `perf fetch`) and `bash scripts/minikube-e2e.sh` (builds the
images, installs with Helm, runs a 2-worker LoadTest against the in-cluster mock API).

To run the operator from a laptop against a cluster during development: `KUBE_CONTEXT=<context> java -cp ... OperatorMain`.

---

## perf-dashboard (web UI)

Like Spark's UI + history server, for load tests: every run, its phases, **live metrics per worker** while it
runs, the final numbers, pod logs, and the Gatling HTML report opened straight from S3/MinIO. Design:
[docs/perf-dashboard-design.md](docs/perf-dashboard-design.md). Code: `perf-dashboard/` (Flask + React, same stack
as the spark-dashboard).

| Page | Shows |
|---|---|
| Runs | every LoadTest: phase, result, workers done/total, start, duration, report link |
| Run detail | steps (validate → setup → workers → report), cards (requests, errors %, req/s, p95, users, SLAs), live throughput + users charts, per-request and per-transaction tables, errors, **workers table** (pod, node, state, restarts, requests, live users / p95, log viewer), the LoadTest spec, Stop & delete |
| New run | form → LoadTest from a saved test plan (workers, CPU/memory, env, secrets, start delay, live metrics). Git and ConfigMap sources are for `perf submit`, CI and GitOps, not the dashboard |
| Test plans | suites of test files in S3 (`s3://perf-results/test-plans/<suite>/`): create (with a starter plan), upload files, a folder or a zip (folders kept), **import a LoadRunner VuGen script** (converted to a new suite, notes shown), edit YAML/CSV in the browser (YAML syntax checked on save), delete, download as zip, start a run from a plan |
| History | every run folder in the results bucket with its final numbers and report link; **Delete** removes the run's folder from S3 (refused while its run is active) |

Deleting from *Run detail* removes the LoadTest (Jobs, pods); its results stay in S3 unless *also delete results in
S3* is ticked. Deleting results cannot be undone — for routine clean-up prefer a bucket lifecycle rule (e.g. expire
`runs/` after 90 days). The dashboard's S3 credentials need write and delete rights on `test-plans/` and `runs/`.

```bash
docker build -t labuk-perf-dashboard:1.0.0 perf-dashboard            # build the runner image first (Java + converter come from it)
helm upgrade --install perf-dashboard charts/perf-dashboard -n perf   --set results.url=s3://perf-results/runs --set results.s3Endpoint=http://minio.minio.svc:9000   --set results.credentialsSecret=minio-creds                          # Secret with AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
kubectl -n perf port-forward svc/perf-dashboard 8080:80                # or --set ingress.enabled=true (host perf-dashboard.codelabuk.dev)
```

### Without Kubernetes (local mode)

The same dashboard runs on **one machine with only Java 17+ and Python 3.10+** — no Kubernetes, no Docker, no
MinIO. Tests run as local `perf` processes (validate → setup → N workers → merged report, the same commands the
operator's Jobs run); results and test plans live in a folder with the same layout as in MinIO.

```bash
# from a checkout (after mvn -pl gatling-runner -am package and npm run build in frontend/):
cd perf-dashboard && start.cmd       # Windows   (./start.sh on Linux / macOS) -> http://127.0.0.1:5000, data in .\perf-data
start.cmd --port 8080 --data D:\perf-data

# for another machine: one package with the UI, the backend and the perf distribution (~72 MB)
cd perf-dashboard && python package_local.py          # -> build/perf-dashboard-local.zip; unzip there, then start.cmd
```

The first start creates a Python environment and installs `requirements-local.txt` (or from `wheels/` with
`package_local.py --with-wheels`, for machines without internet). From a checkout: `python local_main.py`.

| | Local mode |
|---|---|
| Runs | each worker is a process (at most `LOCAL_MAX_WORKERS`, default 8); memory per worker sets its Java heap (75 %); live metrics from a port per worker |
| Storage | `--data` folder: `perf-results/test-plans/<suite>/…` and `perf-results/runs/<run>/…` (tests copy, logs, report, summary) |
| Not available | spreading over nodes, node selection, CPU limits, runner image (Kubernetes features); Git / ConfigMap plan sources |
| Restart | stopping the dashboard stops the tests that are running (marked Failed on the next start); finished results stay |
| Network | listens on 127.0.0.1 by default — `--host 0.0.0.0` lets other machines in (the dashboard has no login) |

Use it to build and try test plans and for small / medium tests; one machine's CPU is shared by every worker, so big
distributed tests belong on the Kubernetes dashboard.

How the numbers work:
- **live** (while Running): the dashboard scrapes each worker's `/metrics` (runs need `metrics.enabled`, on by
  default for runs started from the dashboard) and sums them; finished workers keep counting with their last
  numbers, so totals never go down. p95 here is an estimate from histogram buckets. Small tests update in steps
  (Gatling writes its log in chunks).
- **final** (Completed): `perf worker` writes `logs/worker-<i>.json` and `perf report` writes
  `report/summary.json` next to the report; the dashboard reads those. Percentiles are computed exactly like
  Gatling's report (same t-digest), so the dashboard and the HTML report show the same numbers.

Limits (v1): on Kubernetes, results must be in S3 / MinIO (PVC results show status and pods only); **no login** —
anyone who can open it can edit plans and delete results, so keep it behind your ingress / network rules or put SSO in
front of it.

Developing it: `cd perf-dashboard && python -m venv .venv && .venv/Scripts/pip install -r requirements.txt pytest`
then `python -m pytest tests` (includes a real local run with the perf distribution); UI: `cd frontend && npm install
&& npm run dev` (proxies `/api` to a local `python app.py` with `KUBE_CONTEXT=...`, or to `python local_main.py`).

---

## The `perf` command (developers and CI)

Testers use the [perf-dashboard](#perf-dashboard-web-ui). The `perf` command is for framework developers
(debugging a plan locally, converting LoadRunner scripts) and CI pipelines; it is also what runs inside every pod of
a distributed run, and what the dashboard's local mode starts.

Build once (framework team): `mvn install` → `gatling-runner/target/perf-1.0-SNAPSHOT.zip`.
Unzip it anywhere; only Java 17+ is needed.

```bash
perf validate plans/orders.yaml --workers 4          # check a plan, see the per-worker split
perf run      plans/orders.yaml -DBASE_URL=https://sit.api.example.com
perf run      plans/orders.yaml --scenario checkout --results D:/perf-results
```
(Windows: `perf.cmd ...`; Git Bash works with `perf` too.)

| Command | What it does |
|---|---|
| `perf validate <plan> [--workers N]` | Checks the plan without sending traffic; prints the split per worker |
| `perf run <plan> [options] [-DNAME=value]` | Runs it on this machine; HTML report under `results/` |
| `perf setup <plan> --out shared.json` | Runs only `setup` and saves its shared lists (distributed runs) |
| `perf worker` | One worker of a distributed run; settings from env (`PLAN`, `WORKER_INDEX`/`JOB_COMPLETION_INDEX`, `WORKER_COUNT`, `START_AT`, `SHARED_IN`, `RESULTS_DIR`); writes `worker-<i>.log` |
| `perf report --input <logs> [--output <dir>]` | Merges workers' logs → one report + SLA verdict |
| `perf submit <plan> --workers N ...` | Starts a distributed run on Kubernetes (no LoadTest YAML to write) — see [On Kubernetes](#on-kubernetes-loadtest-operator) |
| `perf status <run> [-n ns] [--watch]` | Follows it; with `--watch` the exit code is the verdict |
| `perf fetch <s3://folder> [<dir>] [--snapshot <s3://folder>]` | Downloads a report / logs / test folder from S3 or MinIO (keeps the folder structure); `--snapshot` also copies what was downloaded to another S3 folder (the operator uses it to freeze a run's tests) |
| `perf convert-lr <VuGen script dir> [--out <dir>]` | Converts a LoadRunner web script to a YAML plan — see [LoadRunner scripts](#loadrunner-scripts-perf-convert-lr) |

Options for `run` / `worker`: `--scenario a,b`, `--results <dir>`, `--workers N --worker-index i`,
`--start-at <time>`, `--shared-in <file>`, `--no-reports`, `--description <text>`, `--metrics-port <port>`,
`-DNAME=value`. Environment: `PERF_HEAP` (default `2g`), `PERF_JAVA_OPTS`, `JAVA_HOME`.
**Exit codes:** `0` OK · `1` error or invalid plan · `2` SLAs failed — use them in CI
(examples: `docs/ci/Jenkinsfile`, `docs/ci/gitlab-ci.yml`).

**Results in S3 / MinIO** — every path option also takes `s3://bucket/prefix/...`:
`perf setup --out s3://.../shared.json`, `SHARED_IN=s3://...`, `RESULTS_DIR=s3://.../logs`,
`perf report --input s3://.../logs --output s3://.../report`. Credentials and endpoint come from the standard
variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (`AWS_SESSION_TOKEN`), `AWS_REGION`, and `S3_ENDPOINT`
for MinIO (e.g. `http://minio.perf:9000`). Built-in client (no AWS SDK): access keys only — EKS IRSA /
web-identity roles are not supported.

The zip contains `bin/` (launchers), `lib/` (engine, Gatling, SQL support, Oracle + PostgreSQL drivers),
`drivers/` (drop extra JDBC jars here) and `conf/logback.xml` (logging).

**Docker image** — plain `docker build` (verified) or Jib (no Docker daemon needed, good for pushing from CI):
```bash
mvn -pl gatling-runner -am package
docker build -t labuk-perf-runner:1.0-SNAPSHOT gatling-runner                     # Dockerfile path
mvn -pl gatling-runner jib:build -Dimage=<registry>/labuk-perf-runner              # or Jib, straight to a registry
docker run --rm -v "$PWD/perf-tests:/tests" labuk-perf-runner:1.0-SNAPSHOT run /tests/plans/orders.yaml
```
The image runs as a non-root user, sizes its heap from the container memory limit (75%), reads extra
drivers from `/opt/perf/drivers/`, and writes worker results to `RESULTS_DIR` (default `/tmp/results` —
mount a volume there, or use `s3://`). (On the dev laptop Jib's hand-off to Docker Desktop stalled, so the
Dockerfiles are the tested local path.)

**Live metrics** — `perf run plans/x.yaml --metrics-port 9464` (or `METRICS_PORT`; with Maven
`-Dmetrics.port=9464`) serves Prometheus metrics on `http://<host>:9464/metrics` while the test runs:

| Metric | Labels |
|---|---|
| `perf_requests_total` | `request` (incl. transaction path), `status` OK/KO — API calls, SQL steps, poll results |
| `perf_response_time_ms` (histogram) | `request` — use `histogram_quantile(0.95, ...)` for p95 |
| `perf_transactions_total`, `perf_transaction_duration_ms` | `transaction`, `status` |
| `perf_users_active` (gauge), `perf_users_started_total` | `scenario` |
| `perf_errors_total` | `message` (top 50) |

Every series has `plan` and `worker` labels, so dashboards sum across workers. A Grafana dashboard is in
`docs/grafana/perf-dashboard.json` (import it, pick the Prometheus data source).
How it works: Gatling's open-source edition cannot stream metrics, so each worker follows its own
`simulation.log` as it is written — the totals match the final report exactly. Gatling writes that log in
chunks, so the numbers trail by ~1 s at hundreds of req/s and by up to ~20 s at a few req/s (`perf` halves the
chunk to 4 KB when metrics are on; with Maven add `-Dgatling.data.file.bufferSize=4096`). After the load ends
a worker keeps serving its final values for 15 s (`METRICS_LINGER_SECONDS`) so the last scrape sees them.
On Kubernetes set `spec.metrics.enabled: true` in the LoadTest: worker pods get the port and
`prometheus.io/scrape` annotations.

Gatling's anonymous usage analytics are switched off in `perf` and in the Maven runs.

---

## LoadRunner / JMeter → this framework

| LoadRunner | JMeter | Here |
|---|---|---|
| Vuser script | Thread group | `scenario` |
| `lr_start/end_transaction` | Transaction controller | `transaction` |
| Parameter file | CSV Data Set | `dataSources` + `feeders` |
| `web_reg_save_param` (correlation) | Extractors | `extract` |
| Think time / pacing | Timers | `wait` / `pace` |
| `lr_rendezvous` | Synchronizing timer | `rendezvous` |
| Controller schedule / goal-oriented | Thread group schedule / throughput timer | `load` / `throttle` |
| SLA | Assertions + plugins | `sla` |
| Load generators / Controller | Master / slave (remote engines) | Workers + merge on Kubernetes (operator) or by hand — [Distributed runs](#distributed-runs--reference) |
| Analysis | HTML report / listeners | One merged Gatling report per run, the dashboard, History |
| VuGen script | `.jmx` | YAML plan — `perf convert-lr` / *Import LoadRunner script* converts VuGen web scripts |

---

## Troubleshooting

- **`"${X}" is not set`** — pass `-DX=...` or set environment variable `X`, or give a default `${X:value}`.
- **Every request after login fails with 401** — check `auth.token` points at the token in the login
  response; `- log: "#{token}"` after a login helps.
- **Users stop immediately** — a `queue` data source / share ran out; the warning at start-up says how many
  users it can serve.
- **Run fails in Validating with "file not found"** — the plan's `include` / data paths are relative to the file
  they are written in; check the suite's layout on the *Test plans* page (a zip of a folder is unpacked without
  that folder's name).
- **A run stays in *Running* / a worker is Pending** — the worker cannot be scheduled: CPU / memory requests do not
  fit the nodes, or `spread: hard` with fewer nodes than workers. `kubectl describe pod <worker>` shows the reason.
- **One worker has far fewer requests** — too little `queue` data, fewer users than workers, or it started late; see
  [How the load and the data are split](#how-the-load-and-the-data-are-split).
- **The dashboard shows no suites in local mode** — local mode has its own data folder (`perf-data`); create or upload
  a suite on its *Test plans* page.
- **See request/response details for failures** — in `logback-test.xml` set
  `io.gatling.http.engine.response` to `TRACE` (failed requests) or `DEBUG` (all requests).

---

## For framework developers

Engine code: `gatling-api-core/src/main/scala/org/codelabuk/perf/`

| Package | Responsibility |
|---|---|
| `load` | YAML → model, placeholders, validation |
| `model` | The plan model (no Gatling types) |
| `expr` | Conditions and durations |
| `data` | CSV, record sources, worker slicing, setup shares |
| `build` | Model → Gatling chains, auth, load, SLAs |
| `sim` | `YamlSimulation` — the single generic simulation |
| `cli` | `Validate` and `Merge` command-line tools |
| `results` | `LogMerger` — combines workers' `simulation.log` files |
| `metrics` | `LiveLogReader`, `LiveMetrics` (Prometheus endpoint), `RunSummary` (exact final numbers) |
| `plugin` | Extension points: `StepPlugin`, `DataSourcePlugin` (via `META-INF/services`) |

A new step kind or protocol is a plugin module on the classpath; existing plans keep working.
`gatling-sql` and `gatling-flight` are the reference examples: `SqlStepPlugin` / `JdbcDataSourcePlugin` and
`FlightStepPlugin` / `FlightDataSourcePlugin`, registered in `src/main/resources/META-INF/services/`. Blocking clients
(JDBC, Flight) run on their own thread pools, never on Gatling's.

**Build and test everything:** `mvn install` (engine, SQL, Flight, runner and operator tests — about 90 JVM tests,
including real Gatling runs), then `cd perf-dashboard && .venv/Scripts/python -m pytest tests`. Netty and gRPC versions
are aligned for all modules in the parent `pom.xml` (one Netty — Gatling's — and gRPC 1.76).

### Running plans with Maven

```bash
# build + unit tests
mvn -pl gatling-api-core test

# demo: a sample plan against a built-in mock API (no network needed)
mvn -pl gatling-api-core test-compile gatling:test -DskipTests \
    -Dgatling.simulationClass=org.codelabuk.perf.it.MockedYamlSimulation

# check a plan without running it (and see how it splits across 4 workers)
mvn -pl gatling-api-core test-compile exec:exec@validate -Dperf.args="D:/perf-tests/plans/orders.yaml --workers 4"

# run YOUR plan
mvn -pl gatling-api-core test-compile gatling:test -DskipTests -Dplan=D:/perf-tests/plans/orders.yaml

# what the Kubernetes operator will do (setup once, 2 workers, merged report), locally against the mock
bash scripts/local-distributed-demo.sh
```

The HTML report path is printed at the end of each run (`gatling-api-core/target/gatling/<run>/index.html`).
The demo plan lives in [`gatling-api-core/src/test/resources/plans`](gatling-api-core/src/test/resources/plans) —
it is the best place to start copying from.

#### Run options

| Option | Meaning |
|---|---|
| `-Dplan=path.yaml` | **Required.** The plan to run. Relative paths resolve from where you run `mvn`. (env: `PLAN`) |
| `-Dscenario=a,b` | Run only these scenarios of the plan. (env: `SCENARIO`) |
| `-Dperf.heap=4g` | Load generator memory (default `2g`). Keep it to about half the machine's RAM. |
| `-DNAME=value` | Fills `${NAME}` placeholders in the YAML — environments, secrets. Environment variables work too. |
| `-Dworker.index=0 -Dworker.count=1` | Distributed runs — which slice of the load this JVM runs. (env: `WORKER_INDEX` / `JOB_COMPLETION_INDEX`, `WORKER_COUNT`) |
| `-Dstart.at=...` | Wait until this moment before starting load — epoch millis or ISO-8601 (`2026-11-20T10:00:00Z`). (env: `START_AT`) |
| `-Dmode=setup -Dshared.out=f.json` | Run only the plan's `setup` and write its shared lists to a file. (env: `MODE`, `SHARED_OUT`) |
| `-Dshared.in=f.json` | Skip `setup`; take the shared lists from that file. (env: `SHARED_IN`) |

One run = one plan. A plan can contain several scenarios that run in parallel.
See [By hand](#by-hand-without-the-operator) for how the last four fit together.

#### Validating a plan

```bash
mvn -pl gatling-api-core test-compile exec:exec@validate -Dperf.args="plans/orders.yaml --workers 4 --scenario checkout"
```

Loads the plan exactly like a run would (includes, CSV files, `${...}` placeholders, conditions, SLA targets)
without sending any traffic, then prints how users and data split across the workers and warns when a
`queue` data source would run out. Exit code 0 = valid, 1 = invalid — use it in CI on every merge request.

---

## Further documents

| Document | What |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How the pieces fit together, design decisions, security, known limits — with diagrams |
| [docs/architecture/](docs/architecture/) | The diagrams as Mermaid (`.mmd`) and PlantUML (`.puml`) sources |
| [docs/perf-dashboard-design.md](docs/perf-dashboard-design.md) | The dashboard: pages, API, test plans, local mode, deletion |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Status of every module, what was verified, open items |
| [docs/ci/](docs/ci/) | Jenkins and GitLab examples |
| [docs/grafana/](docs/grafana/) | Grafana dashboard for the live metrics |
