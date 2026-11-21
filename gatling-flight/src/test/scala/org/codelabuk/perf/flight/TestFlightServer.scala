package org.codelabuk.perf.flight

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{BigIntVector, TimeStampMicroTZVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

/**
 * A small Flight server shaped like labukDB's (JSON commands, one endpoint per data file, a combined endpoint for
 * orderBy / limit, GetSchema with the partition column, DoPut with partitionValues). Table `logs`: 3 files x 10 rows,
 * level = INFO / WARN / ERROR in turn, amount = row number, event_ts = 2026-11-22T00:00:00Z + n minutes.
 */
object TestFlightServer {
  val Files = 3
  val RowsPerFile = 10
  private val Levels = Vector("INFO", "WARN", "ERROR")
  private val Base = Instant.parse("2026-11-22T00:00:00Z")
  private val json = new ObjectMapper()

  val schema: Schema = new Schema(List(
    new Field("level", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
    new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null),
    new Field("event_ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")), null),
    new Field("message", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
  ).asJava)

  /** One DoPut as the server saw it. */
  final case class Put(command: JsonNode, columns: List[String], rows: Vector[Map[String, Any]], batches: Int)
  val puts = new ConcurrentLinkedQueue[Put]()

  private var allocator: BufferAllocator = _
  private var server: FlightServer = _

  def start(port: Int = 0): Int = synchronized {
    if (server == null) {
      allocator = new RootAllocator(Long.MaxValue)
      server = FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", port), Producer).build().start()
    }
    server.getPort
  }

  def stop(): Unit = synchronized {
    if (server != null) {
      server.close(); allocator.close()
      server = null
    }
  }

  private final case class Row(level: String, amount: Long, ts: Instant)
  private def file(i: Int): Vector[Row] = (0 until RowsPerFile).toVector.map { r =>
    val n = i * RowsPerFile + r
    Row(Levels(n % 3), n.toLong, Base.plusSeconds(60L * n))
  }

  private def invalid(msg: String) = CallStatus.INVALID_ARGUMENT.withDescription(msg).toRuntimeException

  private def table(cmd: JsonNode): String = {
    val t = Option(cmd.get("table")).map(_.asText).getOrElse(throw invalid("'table' is required"))
    if (t != "logs") throw invalid(s"unknown table '$t'")
    t
  }

  private object Producer extends NoOpFlightProducer {

    override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
      val cmd = json.readTree(descriptor.getCommand)
      table(cmd)
      val combined = cmd.has("limit") || cmd.has("orderBy")
      val tickets = if (combined) List(cmd.toString) else (0 until Files).toList.map(i => s"$i|${cmd.toString}")
      val endpoints = tickets.map(t => new FlightEndpoint(new Ticket(t.getBytes(UTF_8))))
      new FlightInfo(schema, descriptor, endpoints.asJava, -1, -1)
    }

    override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
      val text = new String(ticket.getBytes, UTF_8)
      val (files, cmd) = text.split("\\|", 2) match {
        case Array(i, c) if i.forall(_.isDigit) => (List(i.toInt), json.readTree(c))
        case _                                    => ((0 until Files).toList, json.readTree(text))
      }
      val level = Option(cmd.get("predicate")).flatMap(p => Option(p.get("literal"))).map(_.asText)
      var rows = files.flatMap(file).filter(r => level.forall(_ == r.level))
      if (cmd.has("orderBy")) rows = rows.sortBy(-_.amount)
      Option(cmd.get("limit")).foreach(l => rows = rows.take(l.asInt))
      Option(cmd.get("delayMs")).foreach(d => Thread.sleep(d.asLong))

      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        listener.start(root)
        rows.grouped(4).foreach { batch => // several batches per stream
          root.allocateNew()
          val lv = root.getVector("level").asInstanceOf[VarCharVector]
          val am = root.getVector("amount").asInstanceOf[BigIntVector]
          val ts = root.getVector("event_ts").asInstanceOf[TimeStampMicroTZVector]
          val msg = root.getVector("message").asInstanceOf[VarCharVector]
          batch.zipWithIndex.foreach { case (r, i) =>
            lv.setSafe(i, r.level.getBytes(UTF_8)); am.setSafe(i, r.amount)
            ts.setSafe(i, r.ts.getEpochSecond * 1000000L); msg.setSafe(i, s"event ${r.amount}".getBytes(UTF_8))
          }
          root.setRowCount(batch.size)
          listener.putNext()
        }
        listener.completed()
      } finally root.close()
    }

    override def getSchema(context: FlightProducer.CallContext, descriptor: FlightDescriptor): SchemaResult = {
      table(json.readTree(descriptor.getCommand))
      new SchemaResult(schema)
    }

    override def acceptPut(context: FlightProducer.CallContext, stream: FlightStream,
                           ack: FlightProducer.StreamListener[PutResult]): Runnable = () => {
      try {
        val cmd = json.readTree(stream.getDescriptor.getCommand)
        table(cmd)
        if (!cmd.has("partitionValues")) throw invalid("'partitionValues' is required")
        var rows = Vector.empty[Map[String, Any]]
        var batches = 0
        var columns = List.empty[String]
        while (stream.next()) {
          val root = stream.getRoot
          columns = root.getSchema.getFields.asScala.map(_.getName).toList
          batches += 1
          rows ++= (0 until root.getRowCount).map(i => root.getFieldVectors.asScala.map(v => v.getName -> v.getObject(i)).toMap)
        }
        puts.add(Put(cmd, columns, rows, batches))
        ack.onNext(PutResult.empty())
        ack.onCompleted()
      } catch { case e: FlightRuntimeException => ack.onError(e) }
    }
  }
}
