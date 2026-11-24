package org.codelabuk.perf.unit

import java.nio.file.{Files, Paths, StandardOpenOption}

import scala.util.Random

import org.codelabuk.perf.metrics.{LiveLogReader, LiveMetrics, LogEvent}
import org.codelabuk.perf.results.LogMerger
import org.scalatest.funsuite.AnyFunSuite

/** Uses a real Gatling 3.13.1 simulation.log (worker 1 of the 2-worker SR flow run). */
class MetricsSpec extends AnyFunSuite {

  private val sample = Paths.get(getClass.getResource("/logs/worker-0.simulation.log").toURI)
  private val bytes = Files.readAllBytes(sample)

  private def readAll(): Vector[LogEvent] = { val r = new LiveLogReader(sample); try r.poll() finally r.close() }

  test("reads a whole log: same users and requests as the merger counts") {
    val events = readAll()
    val merged = LogMerger.merge(Seq(sample), Files.createTempDirectory("m").resolve("simulation.log"))
    assert(events.head.isInstanceOf[LogEvent.Run])
    assert(events.count { case LogEvent.User(_, start, _) => start; case _ => false } == merged.users)
    assert(events.count(_.isInstanceOf[LogEvent.Request]) == merged.requests)
    val names = events.collect { case r: LogEvent.Request => (r.groups :+ r.name).mkString(" / ") }.toSet
    assert(names.contains("sr-end-to-end / getSrStatus") && names.contains("login"), names)
  }

  test("follows a log while it is being written: chunks cut mid-record lose and repeat nothing") {
    val whole = readAll()
    (1 to 5).foreach { seed =>
      val rnd = new Random(seed)
      val f = Files.createTempFile("growing", ".log")
      val reader = new LiveLogReader(f)
      val seen = Vector.newBuilder[LogEvent]
      var pos = 0
      while (pos < bytes.length) {
        val n = math.min(bytes.length - pos, 1 + rnd.nextInt(300))
        Files.write(f, java.util.Arrays.copyOfRange(bytes, pos, pos + n), StandardOpenOption.APPEND)
        pos += n
        seen ++= reader.poll()
      }
      reader.close()
      assert(seen.result() == whole, s"seed $seed")
    }
  }

  test("run summary: exact totals and per-request percentiles from the log") {
    val s = org.codelabuk.perf.metrics.RunSummary.fromLog(sample, Map("worker" -> 0))
    val events = readAll()
    val requests = events.collect { case r: LogEvent.Request => r }
    val total = s.get("total").asInstanceOf[java.util.Map[String, Any]]
    assert(total.get("count") == requests.size.toLong)
    assert(total.get("ko") == requests.count(!_.ok).toLong)
    assert(s.get("users") == events.count { case LogEvent.User(_, true, _) => true; case _ => false }.toLong)
    assert(s.get("worker") == 0)
    val perRequest = s.get("requests").asInstanceOf[java.util.List[java.util.Map[String, Any]]]
    val getStatus = requests.filter(r => (r.groups :+ r.name).mkString(" / ") == "sr-end-to-end / getSrStatus").map(r => r.end - r.start).sorted
    val row = perRequest.toArray.map(_.asInstanceOf[java.util.Map[String, Any]]).find(_.get("name") == "sr-end-to-end / getSrStatus").get
    assert(row.get("count") == getStatus.size.toLong && row.get("max") == getStatus.last && row.get("min") == getStatus.head)
    val p50 = row.get("p50").asInstanceOf[Long]
    assert(p50 >= getStatus.head && p50 <= getStatus.last)
    // written as JSON
    val f = Files.createTempFile("summary", ".json")
    org.codelabuk.perf.metrics.RunSummary.write(s, f)
    assert(new String(Files.readAllBytes(f)).contains("\"requestsPerSecond\""))
  }

  test("run summary reads the whole log in chunks: tiny chunks give exactly the same summary as one read") {
    // a log bigger than one read chunk (16 MB in production) used to be summarised from its first chunk only
    val whole = org.codelabuk.perf.metrics.RunSummary.fromLog(sample)
    val chunked = org.codelabuk.perf.metrics.RunSummary.fromLog(sample, chunkBytes = 64) // ~70 reads of this 4 KB log
    assert(Files.size(sample) > 64 * 10)
    import scala.jdk.CollectionConverters._
    val differs = whole.keySet.asScala.filter(k => whole.get(k) != chunked.get(k))
    assert(differs.isEmpty, differs.map(k => s"$k: whole=${whole.get(k)} chunked=${chunked.get(k)}").mkString("; "))
    val total = chunked.get("total").asInstanceOf[java.util.Map[String, Any]]
    assert(total.get("count") == readAll().count(_.isInstanceOf[LogEvent.Request]).toLong)
  }

  test("Prometheus output: totals match the log, histograms are cumulative") {
    val events = readAll()
    val m = new LiveMetrics("sr-email-status", "0")
    m.record(events)
    val text = m.render()
    val requests = events.count(_.isInstanceOf[LogEvent.Request])
    val total = """(?m)^perf_requests_total\{[^}]*\} (\d+)$""".r.findAllMatchIn(text).map(_.group(1).toLong).sum
    assert(total == requests)
    assert(text.contains("""perf_requests_total{plan="sr-email-status",worker="0",request="login",status="OK"}"""), text.take(600))
    assert(text.contains("# TYPE perf_response_time_ms histogram"))
    // +Inf bucket equals the count, and buckets never decrease
    val getStatus = """(?m)^perf_response_time_ms_bucket\{plan="sr-email-status",worker="0",request="sr-end-to-end / getSrStatus",le="([^"]+)"\} (\d+)$""".r
      .findAllMatchIn(text).map(x => x.group(2).toLong).toList
    assert(getStatus == getStatus.sorted && getStatus.nonEmpty)
    // all users finished at the end of the log
    assert("""(?m)^perf_users_active\{[^}]*\} (-?\d+)$""".r.findAllMatchIn(text).map(_.group(1).toLong).forall(_ == 0))
  }
}
