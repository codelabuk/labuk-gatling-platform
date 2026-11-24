package org.codelabuk.perf.metrics

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, StandardOpenOption}

import scala.collection.mutable

/** Records of a Gatling simulation.log, as the live metrics see them (times in epoch millis). */
sealed trait LogEvent
object LogEvent {
  final case class Run(start: Long, scenarios: Vector[String]) extends LogEvent
  final case class User(scenario: String, start: Boolean, timestamp: Long) extends LogEvent
  final case class Request(groups: List[String], name: String, start: Long, end: Long, ok: Boolean, message: String) extends LogEvent
  final case class Group(groups: List[String], start: Long, end: Long, cumulatedResponseTime: Int, ok: Boolean) extends LogEvent
  final case class Error(message: String, timestamp: Long) extends LogEvent
}

/**
 * Follows a simulation.log WHILE Gatling writes it (same binary format as [[org.codelabuk.perf.results.LogMerger]]).
 * Each [[poll]] returns the records completed since the last poll; a record cut off at the end of the file is left
 * for the next poll, so nothing is lost or read twice.
 */
/** @param chunkBytes most bytes read per poll (16 MB): a big log is read in several polls, see [[readAll]] */
final class LiveLogReader(file: Path, chunkBytes: Int = 16 << 20) extends AutoCloseable {

  private val channel = FileChannel.open(file, StandardOpenOption.READ)
  private var pending = Array.emptyByteArray // bytes read but not yet parsed into complete records
  private val cache = mutable.HashMap.empty[Int, String]
  private var run: Option[LogEvent.Run] = None

  private final class Incomplete extends RuntimeException(null, null, false, false)
  private val incomplete = new Incomplete

  /** Parses whatever complete records are on disk now. */
  def poll(): Vector[LogEvent] = {
    val fresh = ByteBuffer.allocate(math.max(0, (channel.size() - channel.position()).min(chunkBytes.toLong).toInt))
    while (fresh.hasRemaining && channel.read(fresh) > 0) ()
    if (fresh.position() > 0) pending = pending ++ java.util.Arrays.copyOf(fresh.array(), fresh.position())

    val out = Vector.newBuilder[LogEvent]
    var offset = 0
    var continue = true
    while (continue && offset < pending.length) {
      val p = new Parser(pending, offset)
      try {
        val ev = p.record()
        p.commit()
        out += ev
        offset = p.pos
      } catch { case _: Incomplete => continue = false }
    }
    pending = java.util.Arrays.copyOfRange(pending, offset, pending.length)
    out.result()
  }

  /** Whether every byte on disk has been read (a record cut off at the end may still be pending). */
  def atEnd: Boolean = channel.position() >= channel.size()

  /** Reads a complete (finished) log chunk by chunk, handing each event to `f` - memory stays flat for any log size. */
  def readAll(f: LogEvent => Unit): Unit = {
    var events = poll()
    while (events.nonEmpty || !atEnd) {
      events.foreach(f)
      events = poll()
    }
  }

  override def close(): Unit = channel.close()

  /** Parses one record; string-cache additions only take effect on commit (a cut-off record must not leave any). */
  private final class Parser(b: Array[Byte], start: Int) {
    var pos: Int = start
    private val added = mutable.HashMap.empty[Int, String]

    private def need(n: Int): Unit = if (pos + n > b.length) throw incomplete
    def byte(): Byte = { need(1); val v = b(pos); pos += 1; v }
    def bool(): Boolean = byte() != 0
    def int(): Int = { need(4); val v = ByteBuffer.wrap(b, pos, 4).getInt; pos += 4; v }
    def long(): Long = { need(8); val v = ByteBuffer.wrap(b, pos, 8).getLong; pos += 8; v }
    def bytes(): Array[Byte] = { val n = int(); need(n); val v = java.util.Arrays.copyOfRange(b, pos, pos + n); pos += n; v }
    def string(): String = {
      val n = int()
      if (n == 0) ""
      else {
        need(n + 1)
        val raw = java.util.Arrays.copyOfRange(b, pos, pos + n)
        val coder = b(pos + n)
        pos += n + 1
        // Java's internal string bytes: LATIN1, or UTF16 in the platform's (little-endian) order
        new String(raw, if (coder == 0) StandardCharsets.ISO_8859_1 else StandardCharsets.UTF_16LE)
      }
    }
    def cached(): String = {
      val idx = int()
      if (idx >= 0) { val s = string(); added(idx) = s; s }
      else added.get(-idx).orElse(cache.get(-idx)).getOrElse(throw new IllegalStateException(s"$file: unknown cached string #${-idx}"))
    }
    def groups(): List[String] = List.fill(int())(cached())
    def commit(): Unit = cache ++= added

    def record(): LogEvent = {
      val base = run.map(_.start).getOrElse(0L)
      byte() match {
        case 0 =>
          string(); string() // gatling version, simulation class
          val startMs = long()
          string() // description
          val scenarios = Vector.fill(int())(string())
          Vector.fill(int())(bytes()) // assertions
          val r = LogEvent.Run(startMs, scenarios)
          run = Some(r)
          r
        case 1 =>
          val g = groups(); val name = cached(); val s = int(); val e = int(); val ok = bool(); val msg = cached()
          LogEvent.Request(g, name, base + s, base + e, ok, msg)
        case 2 =>
          val sc = int(); val isStart = bool(); val ts = int()
          LogEvent.User(run.flatMap(_.scenarios.lift(sc)).getOrElse(s"scenario-$sc"), isStart, base + ts)
        case 3 =>
          val g = groups(); val s = int(); val e = int(); val cum = int(); val ok = bool()
          LogEvent.Group(g, base + s, base + e, cum, ok)
        case 4 =>
          val msg = cached(); val ts = int()
          LogEvent.Error(msg, base + ts)
        case other => throw new IllegalStateException(s"$file: unknown record type $other - not a Gatling 3.13 simulation.log?")
      }
    }
  }
}
