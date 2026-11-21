package org.codelabuk.perf.flight

import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.util.concurrent.{CompletableFuture, CompletionException, ConcurrentHashMap}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.arrow.flight.{AsyncPutListener, FlightDescriptor, FlightEndpoint, FlightRuntimeException}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.util.Text

/** What a call produced: values to save into the session, and the phase timings (epoch millis). */
final case class FlightOutcome(saved: Map[String, Any], planEnd: Option[Long], firstBatch: Option[Long])

/**
 * Runs one `flight` step on the calling thread (one of the data source's call threads):
 *  - query: GetFlightInfo(command), then DoGet on the chosen endpoints (optionally several at once),
 *  - put: DoPut(command) streaming Arrow batches built from rows, waiting for the server's acknowledgement.
 * Then applies the checks and extractions. Errors are returned as readable text (they become the KO message).
 */
object FlightExecutor {

  private val schemas = new ConcurrentHashMap[(String, String), Schema]() // (data source, GetSchema command) -> schema

  // ------------------------------------------------------------------ query

  private final case class Part(rows: Long, batches: Long, bytes: Long, firstBatch: Option[Long], collected: Collector)

  /** Keeps only what the extractions need, at most maxRows rows. */
  private final class Collector(wanted: Set[String], keepRows: Boolean, maxRows: Int) {
    val values: mutable.LinkedHashMap[String, mutable.ArrayBuffer[Any]] = mutable.LinkedHashMap.empty
    val rows: mutable.ArrayBuffer[Map[String, Any]] = mutable.ArrayBuffer.empty
    var schema: Option[Schema] = None
    private var kept = 0
    def active: Boolean = wanted.nonEmpty || keepRows

    def add(root: VectorSchemaRoot): Unit = if (active && kept < maxRows) {
      if (schema.isEmpty) schema = Some(root.getSchema)
      val vectors = root.getFieldVectors.asScala.map(v => v.getName -> v).toMap
      val n = math.min(root.getRowCount, maxRows - kept)
      for (i <- 0 until n) {
        wanted.foreach(c => vectors.get(c).foreach(v => values.getOrElseUpdate(c, mutable.ArrayBuffer.empty) += read(v, i)))
        if (keepRows) rows += vectors.map { case (k, v) => k -> read(v, i) }
      }
      kept += n
    }

    def merge(other: Collector): Unit = {
      if (schema.isEmpty) schema = other.schema
      other.values.foreach { case (k, vs) => values.getOrElseUpdate(k, mutable.ArrayBuffer.empty) ++= vs }
      rows ++= other.rows
    }
  }

  def query(pool: FlightPool, step: FlightStep, mode: FlightMode.Query, command: String, now: () => Long): Either[String, FlightOutcome] =
    guard {
      val info = pool.client().getInfo(FlightDescriptor.command(command.getBytes(UTF_8)), pool.options: _*)
      val planEnd = now()
      val endpoints = info.getEndpoints.asScala.toVector
      val chosen = mode.fetch match {
        case "all"   => endpoints
        case "first" => endpoints.take(1)
        case _       => Vector.empty
      }
      val wanted = step.extract.collect { case FlightExtract(_, "column" | "columnAll", Some(c), _) => c }.toSet
      val keepRows = step.extract.exists(_.kind == "rows")
      def collector() = new Collector(wanted, keepRows, step.maxRows)

      def fetchOne(ep: FlightEndpoint): Part = {
        val stream = pool.clientFor(ep.getLocations).getStream(ep.getTicket, pool.options: _*)
        try {
          val c = collector()
          var rows, batches, bytes = 0L
          var first: Option[Long] = None
          while (stream.next()) {
            val root = stream.getRoot
            if (first.isEmpty) first = Some(now())
            batches += 1
            rows += root.getRowCount
            bytes += root.getFieldVectors.asScala.map(_.getBufferSize.toLong).sum
            c.add(root)
          }
          if (c.schema.isEmpty) c.schema = Some(stream.getSchema)
          Part(rows, batches, bytes, first, c)
        } finally stream.close()
      }

      val parts: Vector[Part] =
        if (mode.parallel <= 1 || chosen.size <= 1) chosen.map(fetchOne)
        else chosen.grouped(mode.parallel).toVector.flatMap { group =>
          group.map(ep => CompletableFuture.supplyAsync(() => fetchOne(ep), pool.fetchers)).map { f =>
            try f.join() catch { case e: CompletionException if e.getCause != null => throw e.getCause }
          }
        }

      val all = collector()
      parts.foreach(p => all.merge(p.collected))
      if (all.schema.isEmpty) all.schema = Option(info.getSchemaOptional.orElse(null))
      val stats = Map("rows" -> parts.map(_.rows).sum, "batches" -> parts.map(_.batches).sum, "bytes" -> parts.map(_.bytes).sum,
        "endpoints" -> endpoints.size.toLong)
      for {
        _ <- checks(step.checks, stats)
        saved <- extract(step.extract, stats, all)
      } yield FlightOutcome(saved, Some(planEnd), parts.flatMap(_.firstBatch).minOption)
    }

  private def checks(list: List[FlightCheck], stats: Map[String, Long]): Either[String, Unit] =
    list.collectFirst { case c if !compare(stats(c.kind), c.op, c.expected) => s"${c.kind} ${stats(c.kind)} is not ${c.op} ${c.expected}" }
      .toLeft(())

  private def compare(actual: Long, op: String, expected: Long): Boolean = op match {
    case "==" => actual == expected
    case "!=" => actual != expected
    case "<"  => actual < expected
    case "<=" => actual <= expected
    case ">"  => actual > expected
    case ">=" => actual >= expected
  }

  private def extract(list: List[FlightExtract], stats: Map[String, Long], c: Collector): Either[String, Map[String, Any]] = {
    val columns = c.schema.map(_.getFields.asScala.map(_.getName).toList).getOrElse(Nil)
    list.foldLeft[Either[String, Map[String, Any]]](Right(Map.empty)) { (acc, x) =>
      acc.flatMap { saved =>
        def missing(why: String) = if (x.optional) Right(saved) else Left(s"extract ${x.saveAs}: $why")
        x.kind match {
          case "count"     => Right(saved + (x.saveAs -> stats("rows")))
          case "endpoints" => Right(saved + (x.saveAs -> stats("endpoints")))
          case "bytes"     => Right(saved + (x.saveAs -> stats("bytes")))
          case "rows"      => Right(saved + (x.saveAs -> c.rows.toVector))
          case kind =>
            val col = x.column.get
            if (!columns.contains(col)) missing(s"column '$col' not in result (columns: ${columns.mkString(", ")})")
            else {
              val vs = c.values.getOrElse(col, mutable.ArrayBuffer.empty)
              if (kind == "columnAll") Right(saved + (x.saveAs -> vs.toVector))
              else vs.headOption.map(v => Right(saved + (x.saveAs -> v))).getOrElse(missing("no rows"))
            }
        }
      }
    }
  }

  /** Arrow value -> plain JVM value (text as String; timestamps, dates, decimals, lists as Arrow gives them). */
  private def read(v: FieldVector, i: Int): Any = v.getObject(i) match {
    case t: Text => t.toString
    case other   => other
  }

  // ------------------------------------------------------------------ put

  def put(pool: FlightPool, step: FlightStep, mode: FlightMode.Put, command: String, schemaCommand: Option[String],
          rows: Seq[Map[String, Any]]): Either[String, FlightOutcome] =
    guard {
      val full = mode.schema match {
        case Some(cols) => new Schema(cols.map { case (c, t) => field(c, t) }.asJava)
        case None =>
          val cmd = schemaCommand.get
          schemas.computeIfAbsent((pool.source.name, cmd), _ =>
            pool.client().getSchema(FlightDescriptor.command(cmd.getBytes(UTF_8)), pool.options: _*).getSchema)
      }
      val schema = new Schema(full.getFields.asScala.filterNot(f => mode.dropColumns(f.getName)).asJava)
      if (schema.getFields.isEmpty)
        throw new IllegalStateException("the server returned no columns for the table (no data files yet?) - give the columns with `schema:`")
      val names = schema.getFields.asScala.map(_.getName).toSet
      rows.iterator.flatMap(_.keys).find(k => !names(k)).foreach { k =>
        throw new IllegalArgumentException(s"row column '$k' is not in the table's write schema (${names.toList.sorted.mkString(", ")})")
      }
      var batches, bytes = 0L
      val root = VectorSchemaRoot.create(schema, pool.allocator)
      try {
        val writer = pool.client().startPut(FlightDescriptor.command(command.getBytes(UTF_8)), root, new AsyncPutListener(), pool.options: _*)
        try {
          rows.grouped(mode.batchRows).foreach { batch =>
            root.allocateNew()
            batch.zipWithIndex.foreach { case (row, i) =>
              root.getFieldVectors.asScala.foreach(v => set(v, i, row.getOrElse(v.getName, null)))
            }
            root.setRowCount(batch.size)
            bytes += root.getFieldVectors.asScala.map(_.getBufferSize.toLong).sum
            writer.putNext()
            batches += 1
          }
          writer.completed()
          writer.getResult() // waits for the server's acknowledgement (or its error)
        } catch {
          case NonFatal(e) => try writer.error(e) catch { case NonFatal(_) => () }; throw e
        }
      } finally root.close()
      val stats = Map("rows" -> rows.size.toLong, "batches" -> batches, "bytes" -> bytes, "endpoints" -> 0L)
      for {
        _ <- checks(step.checks, stats)
        saved <- extract(step.extract, stats, new Collector(Set.empty, keepRows = false, 0))
      } yield FlightOutcome(saved, None, None)
    }

  private def field(name: String, t: String): Field = {
    val arrow: ArrowType = t match {
      case "string"    => ArrowType.Utf8.INSTANCE
      case "long"      => new ArrowType.Int(64, true)
      case "int"       => new ArrowType.Int(32, true)
      case "short"     => new ArrowType.Int(16, true)
      case "byte"      => new ArrowType.Int(8, true)
      case "double"    => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case "float"     => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case "boolean"   => ArrowType.Bool.INSTANCE
      case "timestamp" => new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")
      case "timestampntz" => new ArrowType.Timestamp(TimeUnit.MICROSECOND, null) // no time zone
      case "date"      => new ArrowType.Date(DateUnit.DAY)
    }
    new Field(name, FieldType.nullable(arrow), null)
  }

  /** Sets one value, converting YAML / session values to the column's Arrow type; null (or "" for non-text) = null. */
  private def set(v: FieldVector, i: Int, value: Any): Unit = {
    val isText = v.isInstanceOf[VarCharVector] || v.isInstanceOf[LargeVarCharVector]
    if (value == null || (!isText && value.toString.trim.isEmpty)) {
      v match {
        case b: BaseFixedWidthVector    => b.setNull(i)
        case b: BaseVariableWidthVector => b.setNull(i)
        case b: BaseLargeVariableWidthVector => b.setNull(i)
        case other => fail(other, value)
      }
      return
    }
    def long: Long = value match {
      case n: Number => n.longValue
      case s         => s.toString.trim.toLong
    }
    def double: Double = value match {
      case n: Number => n.doubleValue
      case s         => s.toString.trim.toDouble
    }
    try v match {
      case x: VarCharVector      => x.setSafe(i, value.toString.getBytes(UTF_8))
      case x: LargeVarCharVector => x.setSafe(i, value.toString.getBytes(UTF_8))
      case x: BigIntVector       => x.setSafe(i, long)
      case x: IntVector          => x.setSafe(i, Math.toIntExact(long))
      case x: SmallIntVector     => x.setSafe(i, Math.toIntExact(long).toShort)
      case x: TinyIntVector      => x.setSafe(i, Math.toIntExact(long).toByte)
      case x: Float8Vector       => x.setSafe(i, double)
      case x: Float4Vector       => x.setSafe(i, double.toFloat)
      case x: BitVector          => x.setSafe(i, if (bool(value)) 1 else 0)
      case x: DecimalVector      => x.setSafe(i, new JBigDecimal(value.toString.trim).setScale(x.getScale))
      case x: DateDayVector      => x.setSafe(i, Math.toIntExact(localDate(value).toEpochDay))
      case x: DateMilliVector    => x.setSafe(i, localDate(value).atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli)
      case x: TimeStampVector    => x.setSafe(i, timestamp(value, x.getField.getType.asInstanceOf[ArrowType.Timestamp].getUnit))
      case other                 => fail(other, value)
    } catch {
      case e: IllegalArgumentException if !e.getMessage.startsWith("column ") =>
        throw new IllegalArgumentException(s"column '${v.getName}': cannot use '$value' as ${v.getField.getType} (${e.getMessage})")
      case e: ArithmeticException =>
        throw new IllegalArgumentException(s"column '${v.getName}': '$value' does not fit ${v.getField.getType}")
    }
  }

  private def fail(v: FieldVector, value: Any): Nothing =
    throw new IllegalArgumentException(s"column '${v.getName}': writing ${v.getField.getType} values is not supported (value '$value')")

  private def bool(value: Any): Boolean = value match {
    case b: java.lang.Boolean => b
    case s => s.toString.trim.toLowerCase match {
      case "true" | "1" | "yes"  => true
      case "false" | "0" | "no"  => false
      case other => throw new IllegalArgumentException(s"not a boolean: $other")
    }
  }

  private def localDate(value: Any): LocalDate = value match {
    case d: LocalDate     => d
    case d: LocalDateTime => d.toLocalDate
    case s                => LocalDate.parse(s.toString.trim.take(10))
  }

  /** ISO-8601 text (with or without offset; no offset = UTC), an Instant / LocalDateTime, or a number = epoch millis. */
  private def timestamp(value: Any, unit: TimeUnit): Long = {
    val instant = value match {
      case n: Number        => Instant.ofEpochMilli(n.longValue)
      case t: Instant       => t
      case t: LocalDateTime => t.toInstant(ZoneOffset.UTC)
      case s =>
        val text = s.toString.trim
        if (text.matches("-?\\d+")) Instant.ofEpochMilli(text.toLong)
        else try Instant.parse(text)
        catch { case _: Exception => try LocalDateTime.parse(text).toInstant(ZoneOffset.UTC) catch { case _: Exception => LocalDate.parse(text).atStartOfDay(ZoneOffset.UTC).toInstant } }
    }
    unit match {
      case TimeUnit.SECOND      => instant.getEpochSecond
      case TimeUnit.MILLISECOND => instant.toEpochMilli
      case TimeUnit.MICROSECOND => Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000L), instant.getNano / 1000L)
      case TimeUnit.NANOSECOND  => Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000000L), instant.getNano.toLong)
    }
  }

  // ------------------------------------------------------------------ errors

  /** Turns Flight / gRPC failures into a readable message: "<status code>: <description>". */
  private def guard(body: => Either[String, FlightOutcome]): Either[String, FlightOutcome] =
    try body
    catch {
      case e: FlightRuntimeException =>
        val d = Option(e.status.description).filter(_.nonEmpty).getOrElse(Option(e.getMessage).getOrElse(""))
        Left(s"${e.status.code}: $d")
      case NonFatal(e) => Left(Option(e.getMessage).filter(_.nonEmpty).map(m => s"${e.getClass.getSimpleName}: $m").getOrElse(e.getClass.getSimpleName))
    }
}
