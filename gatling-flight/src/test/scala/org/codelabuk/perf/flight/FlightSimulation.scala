package org.codelabuk.perf.flight

import java.nio.file.Paths

import org.codelabuk.perf.load.RunOptions
import org.codelabuk.perf.sim.YamlSimulation

/**
 * Runs a plan with `flight` steps against [[TestFlightServer]] on port 47470.
 *   mvn -pl gatling-flight test-compile gatling:test -Dgatling.simulationClass=org.codelabuk.perf.flight.FlightSimulation
 */
class FlightSimulation extends YamlSimulation {
  override protected def planPath: String =
    RunOptions.setting("plan", "PLAN").getOrElse(Paths.get(classOf[FlightSimulation].getResource("/plans/flight.yaml").toURI).toString)

  before { TestFlightServer.start(FlightSimulation.Port); () }
  after(TestFlightServer.stop())
}

object FlightSimulation {
  val Port = 47470
}
