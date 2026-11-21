package org.codelabuk.perf.flight

import scala.concurrent.duration._

import org.codelabuk.perf.load.Node
import org.codelabuk.perf.model.{DataSourceDef, TestPlan}

/** A Flight server: `dataSources: { name: { type: flight, uri: grpc://host:port } }`. */
final case class FlightSource(
    name: String,
    uri: String,
    poolSize: Int,           // threads running calls (Flight's Java client blocks) = calls in flight at once
    connections: Int,        // gRPC channels, used round-robin
    headers: Map[String, String],
    timeout: FiniteDuration, // per call
    verifyServer: Boolean    // TLS (grpc+tls://): check the server certificate
)

/** kind: rows | endpoints | batches | bytes ; op: == != < <= > >= */
final case class FlightCheck(kind: String, op: String, expected: Long)

/** kind: column (first row) | columnAll | rows (all rows as maps) | count (rows) | endpoints | bytes */
final case class FlightExtract(saveAs: String, kind: String, column: Option[String], optional: Boolean)

sealed trait PutRows
object PutRows {
  /** Rows written in the YAML; values may be #{...}. Repeated `times` times. */
  final case class Literal(rows: List[List[(String, Either[String, Any])]], times: Int) extends PutRows
  /** A session variable holding a list of maps (e.g. extracted by an earlier step). */
  final case class FromVariable(el: String) extends PutRows
}

sealed trait FlightMode
object FlightMode {
  /** GetFlightInfo(command), then DoGet on all / the first / no endpoint(s). */
  final case class Query(command: String, fetch: String, parallel: Int) extends FlightMode
  /** DoPut(command) with Arrow batches built from `rows`. */
  final case class Put(command: String, rows: PutRows, batchRows: Int, schema: Option[List[(String, String)]],
                       schemaCommand: Option[String], dropColumns: Set[String]) extends FlightMode
}

final case class FlightStep(
    name: String,
    datasource: String,
    mode: FlightMode,
    checks: List[FlightCheck],
    extract: List[FlightExtract],
    maxRows: Int,
    phases: Boolean // also record "<name> (plan)" and "<name> (first batch)"
)

/** Reads the YAML for `type: flight` data sources and `flight` steps - shared by validation and build. */
object FlightModel {

  val Fetch: Set[String] = Set("all", "first", "none")
  /** `timestamp` = an instant (UTC time zone in Arrow); `timestampNtz` = without time zone (e.g. what labukDB's Delta writer expects). */
  val SchemaTypes: Set[String] = Set("string", "long", "int", "short", "byte", "double", "float", "boolean", "timestamp", "timestampntz", "date")
  private val CheckOp = """^\s*(==|!=|<=|>=|<|>)?\s*(-?\d+)\s*$""".r
  private val CheckKinds = List("rows", "endpoints", "batches", "bytes")

  def source(name: String, n: Node): FlightSource = {
    n.allowOnly("type", "uri", "pool", "headers", "timeout", "verifyServer")
    val uri = n.str("uri")
    if (!uri.matches("^(grpc|grpc\\+tcp|grpc\\+tls|grpc\\+unix)://.+"))
      n.get("uri").fail(s"expected grpc://host:port, grpc+tls://host:port or grpc+unix://path - got '$uri'")
    val pool = n.opt("pool").map(_.allowOnly("size", "connections"))
    val size = pool.flatMap(_.optInt("size")).getOrElse(16)
    val connections = pool.flatMap(_.optInt("connections")).getOrElse(math.min(4, size))
    if (size < 1) n.get("pool").get("size").fail("must be at least 1")
    if (connections < 1) n.get("pool").get("connections").fail("must be at least 1")
    FlightSource(name, uri, size, connections, n.optStringMap("headers"), n.optDuration("timeout").getOrElse(60.seconds),
      n.optBool("verifyServer").getOrElse(true))
  }

  /** The node of Flight source `name`, failing (at `at`) if it is missing or another type. */
  def sourceNode(plan: TestPlan, name: String, at: Node): Node =
    plan.dataSources.get(name) match {
      case Some(DataSourceDef.Plugin(_, "flight", n)) => n
      case Some(_) => at.fail(s"'$name' is not a flight data source (`type: flight` with a `uri`)")
      case None =>
        val known = plan.dataSources.collect { case (k, DataSourceDef.Plugin(_, "flight", _)) => k }
        at.fail(s"unknown flight data source '$name' - defined: ${if (known.isEmpty) "none" else known.mkString(", ")}")
    }

  /** Report name of a step: its `name`, or "flight <datasource>". */
  def stepName(v: Node): String = v.optStr("name").getOrElse(s"flight ${v.optStr("datasource").getOrElse("?")}")

  def reportNames(v: Node): Set[String] = {
    val name = stepName(v)
    if (v.optBool("phases").contains(true)) Set(name, s"$name (plan)", s"$name (first batch)") else Set(name)
  }

  /** A command (the Flight descriptor's bytes): a YAML object sent as JSON, or text sent as is. #{...} stay for run time. */
  private def command(n: Node): String = if (n.isObject || n.isArray) n.jsonWithPlaceholders else n.text

  def step(n: Node, plan: TestPlan): FlightStep = {
    val v = n.get("flight")
    v.allowOnly("datasource", "name", "query", "put", "fetch", "parallel", "rows", "times", "batchRows", "schema", "schemaFrom",
      "checks", "extract", "maxRows", "phases")
    val ds = v.str("datasource")
    source(ds, sourceNode(plan, ds, v.get("datasource")))

    val mode: FlightMode = (v.has("query"), v.has("put")) match {
      case (true, true)   => v.fail("use either `query` (GetFlightInfo + DoGet) or `put` (DoPut), not both")
      case (false, false) => v.fail("needs `query:` (read: GetFlightInfo + DoGet) or `put:` (write: DoPut)")
      case (true, false) =>
        List("rows", "times", "batchRows", "schema", "schemaFrom").filter(v.has).foreach(k => v.get(k).fail(s"`$k` is only for `put:`"))
        val fetch = v.optStr("fetch").getOrElse("all")
        if (!Fetch(fetch)) v.get("fetch").fail(s"expected one of ${Fetch.mkString(", ")} - got '$fetch'")
        val parallel = v.optInt("parallel").getOrElse(1)
        if (parallel < 1) v.get("parallel").fail("must be at least 1")
        FlightMode.Query(command(v.get("query")), fetch, parallel)
      case (false, true) =>
        List("fetch", "parallel").filter(v.has).foreach(k => v.get(k).fail(s"`$k` is only for `query:`"))
        val put = v.get("put")
        val rowsNode = v.opt("rows").getOrElse(v.fail("a `put` needs `rows:` - a list of rows, or a #{variable} holding a list of maps"))
        val rows: PutRows =
          if (rowsNode.isArray) {
            val literal = rowsNode.list.map { r =>
              if (!r.isObject) r.fail("each row is a map of column: value")
              r.entries.map { case (col, value) => col -> valueOf(value) }
            }
            if (literal.isEmpty) rowsNode.fail("at least one row is required")
            val times = v.optInt("times").getOrElse(1)
            if (times < 1) v.get("times").fail("must be at least 1")
            PutRows.Literal(literal, times)
          } else {
            if (v.has("times")) v.get("times").fail("`times` repeats literal rows - not used with a #{variable}")
            PutRows.FromVariable(rowsNode.text.trim.stripPrefix("#{").stripSuffix("}").trim)
          }
        val batchRows = v.optInt("batchRows").getOrElse(1000)
        if (batchRows < 1) v.get("batchRows").fail("must be at least 1")
        val schema = v.opt("schema").map(_.entries.map { case (col, t) =>
          val tt = t.text.toLowerCase.replace("_", "")
          if (!SchemaTypes(tt)) t.fail(s"unknown type '$tt' - use ${SchemaTypes.toList.sorted.mkString(", ")}")
          col -> tt
        })
        // without an explicit schema it is asked from the server: GetSchema(schemaFrom), by default the put's table
        val schemaCommand = if (schema.isDefined) None else Some(v.opt("schemaFrom").map(command).getOrElse {
          if (!put.isObject) v.fail("a text `put` needs `schema:` or `schemaFrom:`")
          val keep = put.keys.filter(k => k == "table" || k == "tableRoot")
          if (keep.isEmpty) v.fail("give `schema:` (column: type) or `schemaFrom:` (a descriptor for GetSchema) - the put has no table / tableRoot")
          val m = new com.fasterxml.jackson.databind.ObjectMapper()
          val obj = m.createObjectNode()
          keep.foreach(k => obj.put(k, org.codelabuk.perf.load.Placeholders.resolve(put.str(k), s"${put.path}.$k")))
          m.writeValueAsString(obj)
        })
        // partition columns are fixed by the put (labukDB: partitionValues) and are not part of the batches
        val drop = put.opt("partitionValues").map(_.keys.toSet).getOrElse(Set.empty)
        FlightMode.Put(command(put), rows, batchRows, schema, schemaCommand, drop)
    }

    val checks = v.optList("checks").map { c =>
      val kind = CheckKinds.find(c.has).getOrElse(c.fail(s"expected one of ${CheckKinds.mkString(", ")} with a number, e.g. rows: \">= 1\""))
      c.allowOnly(kind)
      if (mode.isInstanceOf[FlightMode.Put] && kind == "endpoints") c.fail("`endpoints` is for queries")
      c.str(kind) match {
        case CheckOp(op, num) => FlightCheck(kind, Option(op).getOrElse("=="), num.toLong)
        case other            => c.get(kind).fail(s"expected a number with an optional operator, e.g. '>= 1' - got '$other'")
      }
    }
    val extract = v.opt("extract").map(_.entries).getOrElse(Nil).map { case (saveAs, e) =>
      if (e.isText) FlightExtract(saveAs, "column", Some(e.text), optional = false)
      else {
        e.allowOnly("column", "all", "rows", "count", "endpoints", "bytes", "optional")
        val optional = e.optBool("optional").getOrElse(false)
        val flag = List("rows", "count", "endpoints", "bytes").find(k => e.optBool(k).contains(true))
        flag match {
          case Some(k)                => FlightExtract(saveAs, k, None, optional)
          case None if e.has("column") => FlightExtract(saveAs, if (e.optBool("all").contains(true)) "columnAll" else "column", Some(e.str("column")), optional)
          case None                    => e.fail("expected column (+ all: true), rows: true, count: true, endpoints: true or bytes: true")
        }
      }
    }
    if (mode.isInstanceOf[FlightMode.Put] && extract.exists(x => Set("column", "columnAll", "rows", "endpoints")(x.kind)))
      v.get("extract").fail("a put returns no rows - only count: true or bytes: true can be extracted")
    FlightStep(stepName(v), ds, mode, checks, extract, v.optInt("maxRows").getOrElse(1000), v.optBool("phases").getOrElse(false))
  }

  /** YAML numbers and booleans keep their type; text (incl. #{...}) is resolved at run time. */
  private def valueOf(n: Node): Either[String, Any] =
    if (n.raw == null || n.raw.isNull) Right(null)
    else if (n.raw.isIntegralNumber) Right(n.raw.asLong())
    else if (n.raw.isNumber) Right(n.raw.asDouble())
    else if (n.raw.isBoolean) Right(n.raw.asBoolean())
    else if (n.text.contains("#{")) Left(n.text)
    else Right(n.text)
}
