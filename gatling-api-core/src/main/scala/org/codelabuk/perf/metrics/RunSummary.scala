package org.codelabuk.perf.metrics

import java.nio.file.{Files, Path}

import scala.collection.mutable

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}

/**
 * Final numbers of a run (or of one worker) computed exactly from its simulation.log, as small JSON - so tools
 * that cannot read Gatling's binary log (the perf-dashboard) can show results after the pods are gone.
 */
object RunSummary {

  final case class Stats(count: Long, ok: Long, ko: Long, min: Long, mean: Long, p50: Long, p75: Long, p95: Long, p99: Long, max: Long)

  /**
   * Running statistics for one name, computed exactly like Gatling's HTML report (GeneralStatsBuffer in
   * gatling-charts 3.13): a t-digest with compression 100, values added in log order, percentiles rounded; mean
   * rounded. Values go straight into the digest - nothing per request is kept, so memory does not grow with the run.
   */
  private final class Acc {
    private val digest = new com.tdunning.math.stats.AVLTreeDigest(100.0)
    var count, ok, sum = 0L
    def add(duration: Long, isOk: Boolean): Unit = {
      digest.add(duration.toDouble); count += 1; sum += duration; if (isOk) ok += 1
    }
    def stats: Stats =
      if (count == 0) Stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      else {
        def p(rank: Double): Long = math.round(digest.quantile(rank / 100.0))
        Stats(count, ok, count - ok, digest.quantile(0).toLong, math.round(sum.toDouble / count), p(50), p(75), p(95), p(99), digest.quantile(1).toLong)
      }
  }

  /** Summary of a complete log, as a JSON-ready map. Reads the log in chunks (any size). */
  def fromLog(log: Path, extra: Map[String, Any] = Map.empty, chunkBytes: Int = 16 << 20): java.util.Map[String, Any] = {
    val reqs = mutable.LinkedHashMap.empty[String, Acc]
    val txs = mutable.LinkedHashMap.empty[String, Acc]
    val errors = mutable.LinkedHashMap.empty[String, Long]
    val all = new Acc
    var users = 0L
    var start = Long.MaxValue
    var end = 0L

    val reader = new LiveLogReader(log, chunkBytes)
    try reader.readAll {
      case LogEvent.Run(s, _) => start = math.min(start, s)
      case LogEvent.User(_, isStart, ts) => if (isStart) users += 1; end = math.max(end, ts)
      case LogEvent.Request(groups, name, s, e, ok, msg) =>
        reqs.getOrElseUpdate((groups :+ name).mkString(" / "), new Acc).add(e - s, ok)
        all.add(e - s, ok)
        if (!ok && msg.nonEmpty) errors(msg) = errors.getOrElse(msg, 0L) + 1
        end = math.max(end, e)
      case LogEvent.Group(groups, s, e, _, ok) =>
        txs.getOrElseUpdate(groups.mkString(" / "), new Acc).add(e - s, ok)
        end = math.max(end, e)
      case LogEvent.Error(msg, _) => errors(msg) = errors.getOrElse(msg, 0L) + 1
    } finally reader.close()

    def statsMap(st: Stats): java.util.Map[String, Any] = {
      val m = new java.util.LinkedHashMap[String, Any]()
      m.put("count", st.count); m.put("ok", st.ok); m.put("ko", st.ko); m.put("min", st.min); m.put("mean", st.mean)
      m.put("p50", st.p50); m.put("p75", st.p75); m.put("p95", st.p95); m.put("p99", st.p99); m.put("max", st.max)
      m
    }
    def named(src: mutable.LinkedHashMap[String, Acc]): java.util.List[Any] = {
      val list = new java.util.ArrayList[Any]()
      src.foreach { case (k, a) => val m = statsMap(a.stats); m.put("name", k); list.add(m) }
      list
    }

    val durationMs = if (start == Long.MaxValue || end <= start) 0L else end - start
    val total = all.stats
    val out = new java.util.LinkedHashMap[String, Any]()
    extra.foreach { case (k, v) => out.put(k, v) }
    out.put("start", if (start == Long.MaxValue) null else java.time.Instant.ofEpochMilli(start).toString)
    out.put("durationSeconds", durationMs / 1000.0)
    out.put("users", users)
    out.put("total", statsMap(total))
    out.put("requestsPerSecond", if (durationMs == 0) 0.0 else math.round(total.count * 100000.0 / durationMs) / 100.0)
    out.put("errorPercent", if (total.count == 0) 0.0 else math.round(total.ko * 10000.0 / total.count) / 100.0)
    out.put("requests", named(reqs))
    out.put("transactions", named(txs))
    val errs = new java.util.ArrayList[Any]()
    errors.toList.sortBy(-_._2).take(20).foreach { case (m, c) => val e = new java.util.LinkedHashMap[String, Any](); e.put("message", m); e.put("count", c); errs.add(e) }
    out.put("errors", errs)
    out
  }

  private val json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)

  def write(summary: java.util.Map[String, Any], file: Path): Path = {
    Option(file.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    json.writeValue(file.toFile, summary)
    file
  }
}
