package org.codelabuk.perf.flight

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.codelabuk.perf.load.{PlanException, PlanLoader}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FlightSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var pool: FlightPool = _
  private var port = 0

  override def beforeAll(): Unit = {
    port = TestFlightServer.start()
    pool = new FlightPool(FlightSource("db", s"grpc://localhost:$port", 4, 2, Map.empty, 10.seconds, verifyServer = true))
  }
  override def afterAll(): Unit = { pool.close(); TestFlightServer.stop() }

  private val now = () => System.currentTimeMillis()
  private def step(mode: FlightMode, checks: List[FlightCheck] = Nil, extract: List[FlightExtract] = Nil) =
    FlightStep("t", "db", mode, checks, extract, 1000, phases = false)
  private def query(cmd: String, fetch: String = "all", parallel: Int = 1, checks: List[FlightCheck] = Nil, extract: List[FlightExtract] = Nil) = {
    val q = FlightMode.Query(cmd, fetch, parallel)
    FlightExecutor.query(pool, step(q, checks, extract), q, cmd, now)
  }
  private val errors = """{"table":"logs","predicate":{"type":"cmp","column":"level","op":"EQ","literal":"ERROR","literalType":"string"}}"""

  // ---------------------------------------------------------------- reads

  test("query: plan + DoGet on every endpoint, checks and extraction") {
    val r = query(errors, checks = List(FlightCheck("endpoints", "==", 3), FlightCheck("rows", "==", 10), FlightCheck("batches", ">=", 3)),
      extract = List(FlightExtract("first", "column", Some("amount"), optional = false), FlightExtract("levels", "columnAll", Some("level"), optional = false),
        FlightExtract("n", "count", None, optional = false), FlightExtract("eps", "endpoints", None, optional = false),
        FlightExtract("rows", "rows", None, optional = false), FlightExtract("bytes", "bytes", None, optional = false)))
    val out = r.toOption.getOrElse(fail(r.toString))
    assert(out.saved("first") == 2L)                       // first ERROR row of file 0
    assert(out.saved("levels").asInstanceOf[Seq[Any]].toSet == Set("ERROR"))
    assert(out.saved("n") == 10L && out.saved("eps") == 3L)
    assert(out.saved("rows").asInstanceOf[Seq[Map[String, Any]]].head("message") == "event 2")
    assert(out.saved("bytes").asInstanceOf[Long] > 0)
    assert(out.planEnd.isDefined && out.firstBatch.isDefined)
  }

  test("parallel endpoint fetch gives the same result as one at a time") {
    val ex = List(FlightExtract("all", "columnAll", Some("amount"), optional = false))
    val seq = query(errors, extract = ex).toOption.get.saved("all")
    val par = query(errors, parallel = 3, extract = ex).toOption.get.saved("all")
    assert(seq == par && seq.asInstanceOf[Seq[Any]].size == 10)
  }

  test("fetch first / none, and a combined (orderBy + limit) endpoint") {
    assert(query(errors, fetch = "first", checks = List(FlightCheck("rows", "<", 10))).isRight)
    assert(query(errors, fetch = "none", checks = List(FlightCheck("rows", "==", 0), FlightCheck("endpoints", "==", 3))).isRight)
    val top = query("""{"table":"logs","orderBy":[{"column":"amount","ascending":false}],"limit":5}""",
      checks = List(FlightCheck("endpoints", "==", 1), FlightCheck("rows", "==", 5)),
      extract = List(FlightExtract("max", "column", Some("amount"), optional = false)))
    assert(top.toOption.get.saved("max") == 29L)
  }

  test("failures become readable messages") {
    assert(query("""{"table":"nope"}""") == Left("INVALID_ARGUMENT: unknown table 'nope'"))
    assert(query(errors, checks = List(FlightCheck("rows", ">", 10))) == Left("rows 10 is not > 10"))
    assert(query(errors, extract = List(FlightExtract("x", "column", Some("nope"), optional = false))).swap.toOption.get
      .startsWith("extract x: column 'nope' not in result"))
    // no matching rows: an optional extract saves nothing, a required one fails
    val none = """{"table":"logs","predicate":{"literal":"NONE"}}"""
    assert(query(none, extract = List(FlightExtract("x", "column", Some("amount"), optional = true))).map(_.saved) == Right(Map.empty))
    assert(query(none, extract = List(FlightExtract("x", "column", Some("amount"), optional = false))) == Left("extract x: no rows"))
    val down = new FlightPool(FlightSource("down", "grpc://localhost:1", 1, 1, Map.empty, 2.seconds, verifyServer = true))
    try {
      val q = FlightMode.Query(errors, "all", 1)
      val r = FlightExecutor.query(down, step(q), q, errors, now)
      assert(r.isLeft && r.swap.toOption.get.startsWith("UNAVAILABLE"), r)
    } finally down.close()
  }

  // ---------------------------------------------------------------- writes

  private def put(rows: Seq[Map[String, Any]], batchRows: Int = 4, schema: Option[List[(String, String)]] = None,
                  checks: List[FlightCheck] = Nil, extract: List[FlightExtract] = Nil, table: String = "logs") = {
    val cmd = s"""{"table":"$table","partitionValues":{"level":"ERROR"}}"""
    val p = FlightMode.Put(cmd, PutRows.Literal(Nil, 1), batchRows, schema, if (schema.isEmpty) Some(s"""{"table":"$table"}""") else None, Set("level"))
    FlightExecutor.put(pool, step(p, checks, extract), p, cmd, p.schemaCommand, rows)
  }

  test("put: schema from the server minus the partition column, rows converted, batches of batchRows") {
    TestFlightServer.puts.clear()
    val rows = Seq(
      Map("amount" -> 7L, "event_ts" -> "2026-11-22T10:00:00Z", "message" -> "a"),
      Map("amount" -> "8", "event_ts" -> 1795341600000L, "message" -> null),
      Map("amount" -> 9, "event_ts" -> "2026-11-22T10:00:00", "message" -> "c"),
      Map("amount" -> 10L, "message" -> "no timestamp"),
      Map("amount" -> 11L, "event_ts" -> "", "message" -> "e"))
    val r = put(rows, checks = List(FlightCheck("rows", "==", 5), FlightCheck("batches", "==", 2)),
      extract = List(FlightExtract("n", "count", None, optional = false)))
    assert(r.map(_.saved) == Right(Map("n" -> 5L)), r)
    val seen = TestFlightServer.puts.asScala.toList
    assert(seen.size == 1)
    val p = seen.head
    assert(p.columns == List("amount", "event_ts", "message"), "level is fixed by partitionValues, not sent")
    assert(p.batches == 2 && p.rows.size == 5)
    assert(p.rows.map(_("amount")) == Vector(7L, 8L, 9L, 10L, 11L))
    val ts = p.rows.map(_("event_ts"))
    assert(ts(0) == ts(1) && ts(1) == ts(2) && ts(0) != null, ts)   // ISO with Z, epoch millis, ISO without offset (= UTC)
    assert(ts(3) == null && ts(4) == null)
    assert(p.rows(1)("message") == null && p.rows(0)("message").toString == "a")
  }

  test("put with timestampNtz: a timestamp column without time zone (what labukDB's Delta writer expects)") {
    TestFlightServer.puts.clear()
    val r = put(Seq(Map("amount" -> 1L, "event_time" -> "2026-11-22T12:00:00Z")),
      schema = Some(List("amount" -> "long", "event_time" -> "timestampntz")))
    assert(r.isRight, r)
    val ts = TestFlightServer.puts.asScala.head.rows.head("event_time")
    assert(ts == java.time.LocalDateTime.parse("2026-11-22T12:00:00"), s"no time zone: Arrow returns a LocalDateTime, got $ts")
  }

  test("validate works with a flight data source (a connection, not a feeder)") {
    val f = Files.createTempFile("flight-validate", ".yaml")
    Files.write(f, plan("""{ datasource: db, query: { table: logs } }""").getBytes(StandardCharsets.UTF_8))
    val r = org.codelabuk.perf.cli.Validate.run(List(f.toString, "--workers", "2"))
    assert(r.ok, r.report)
    assert(r.report.contains("Split across 2 worker(s)") && !r.report.contains("data db"))
  }

  test("put with an explicit schema, and its errors") {
    TestFlightServer.puts.clear()
    val ok = put(Seq(Map("amount" -> 1L)), schema = Some(List("amount" -> "long", "message" -> "string")))
    assert(ok.isRight, ok)
    assert(TestFlightServer.puts.asScala.head.columns == List("amount", "message"))
    assert(put(Seq(Map("amont" -> 1L))).swap.toOption.get.contains("row column 'amont' is not in the table's write schema"))
    assert(put(Seq(Map("amount" -> "abc"))).swap.toOption.get.contains("column 'amount': cannot use 'abc'"))
    assert(put(Seq(Map("amount" -> 1L)), table = "nope") == Left("INVALID_ARGUMENT: unknown table 'nope'"))
  }

  // ---------------------------------------------------------------- YAML

  private def load(yaml: String) = {
    val f = Files.createTempFile("flight-plan", ".yaml")
    Files.write(f, yaml.getBytes(StandardCharsets.UTF_8))
    PlanLoader.load(f)
  }
  private def plan(step: String, sources: String = "db: { type: flight, uri: \"grpc://localhost:1\" }") =
    s"""name: t
       |baseUrl: http://localhost:1
       |dataSources:
       |  ${sources}
       |scenarios:
       |  - name: s
       |    steps:
       |      - flight: ${step}
       |    load: { profile: [ { atOnceUsers: 1 } ] }
       |""".stripMargin
  private def error(yaml: String) = intercept[PlanException](load(yaml)).getMessage

  test("plans with flight steps load; mistakes are reported with their YAML path") {
    load(plan("""{ datasource: db, name: q, query: { table: logs, predicate: { literal: "#{level}" } }, checks: [ { rows: ">= 1" } ] }"""))
    load(plan("""{ datasource: db, put: { table: logs, partitionValues: { level: ERROR } }, rows: [ { amount: 1 } ], times: 3 }"""))
    assert(error(plan("{ datasource: nope, query: { table: logs } }")).contains("unknown flight data source 'nope'"))
    assert(error(plan("{ datasource: db }")).contains("needs `query:`"))
    assert(error(plan("{ datasource: db, query: { table: a }, put: { table: a } }")).contains("not both"))
    assert(error(plan("{ datasource: db, query: { table: a }, fetch: some }")).contains("expected one of all, first, none"))
    assert(error(plan("{ datasource: db, query: { table: a }, checks: [ { rows: lots } ] }")).contains("expected a number"))
    assert(error(plan("{ datasource: db, query: { table: a }, rows: [ { a: 1 } ] }")).contains("`rows` is only for `put:`"))
    assert(error(plan("{ datasource: db, put: { table: a, partitionValues: {} } }")).contains("a `put` needs `rows:`"))
    assert(error(plan("{ datasource: db, put: { table: a }, rows: [ { a: 1 } ], extract: { x: { column: a } } }")).contains("a put returns no rows"))
    assert(error(plan("{ datasource: db, put: { x: 1 }, rows: [ { a: 1 } ] }")).contains("give `schema:`"))
    assert(error(plan("{ datasource: db, query: { table: a } }", "db: { type: flight, uri: \"http://x\" }")).contains("expected grpc://host:port"))
    assert(error(plan("{ datasource: db, query: { table: a } }", "db: { type: flight, uri: \"grpc://x:1\", pol: {} }")).contains("pol"))
  }
}
