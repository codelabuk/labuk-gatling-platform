package org.codelabuk.perf.flight

import java.nio.file.{Files, Paths}

import scala.jdk.CollectionConverters._

import org.codelabuk.perf.metrics.{LiveLogReader, LogEvent}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Runs plans/flight.yaml through real Gatling (in this JVM) against TestFlightServer and checks every recorded call. */
class FlightRunSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var events: Vector[LogEvent] = Vector.empty
  private var exitCode = -1

  override def beforeAll(): Unit = {
    TestFlightServer.puts.clear()
    val plan = Paths.get(getClass.getResource("/plans/flight.yaml").toURI)
    val results = Files.createTempDirectory("flight-run")
    sys.props ++= Map("plan" -> plan.toString, "gatling.data.enableAnalytics" -> "false")
    try {
      com.typesafe.config.ConfigFactory.invalidateCaches()
      val module = Class.forName("io.gatling.app.Gatling$").getField("MODULE$").get(null)
      exitCode = module.getClass.getMethod("fromArgs", classOf[Array[String]]).invoke(module,
        Array("--simulation", classOf[FlightSimulation].getName, "--results-folder", results.toString, "--no-reports")).asInstanceOf[Int]
    } finally sys.props -= "plan"
    val log = Files.walk(results).iterator().asScala.find(_.getFileName.toString == "simulation.log").getOrElse(fail("no simulation.log"))
    val r = new LiveLogReader(log)
    try events = r.poll() finally r.close()
  }

  private def requests(name: String) = events.collect { case r: LogEvent.Request if r.name == name => r }
  private def ok(name: String) = requests(name).count(_.ok)
  private def ko(name: String) = requests(name).count(!_.ok)

  test("the run completed and the SLAs passed") {
    assert(exitCode == 0, s"Gatling exit code $exitCode")
  }

  test("reads: plan + parallel DoGet per user, checks passed, phases recorded") {
    assert(ok("scan-by-level") == 6 && ko("scan-by-level") == 0, requests("scan-by-level").find(!_.ok).map(_.message))
    assert(ok("scan-by-level (plan)") == 6 && ok("scan-by-level (first batch)") == 6)
    val whole = requests("scan-by-level").map(r => r.end - r.start).sum
    val plan = requests("scan-by-level (plan)").map(r => r.end - r.start).sum
    assert(plan <= whole, "planning is part of the whole call")
    assert(ok("top-5") == 6)
  }

  test("a failing call is a KO with the server's message") {
    assert(ko("unknown-table") == 6)
    assert(requests("unknown-table").head.message.contains("INVALID_ARGUMENT: unknown table 'nope'"))
  }

  test("writes: every put arrived with 10 rows in 3 batches, partition column left out") {
    assert(ok("put-errors") == 4 && ko("put-errors") == 0, requests("put-errors").find(!_.ok).map(_.message))
    val puts = TestFlightServer.puts.asScala.toList
    assert(puts.size == 4)
    assert(puts.forall(p => p.rows.size == 10 && p.batches == 3 && p.columns == List("amount", "event_ts", "message")))
    assert(puts.flatMap(_.rows.map(_("message").toString)).distinct.size == 4 * 5 + 1, "a new UUID per row + 'second'")
  }
}
