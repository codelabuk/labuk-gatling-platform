package org.codelabuk.perf.flight

import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import org.codelabuk.perf.build.BuildContext
import org.codelabuk.perf.load.Node
import org.codelabuk.perf.model.TestPlan
import org.codelabuk.perf.plugin.{DataSourcePlugin, StepPlugin}

/** The `flight:` step. Registered via META-INF/services, so it is available whenever this module is on the classpath. */
final class FlightStepPlugin extends StepPlugin {
  override val kind: String = "flight"

  override def validate(node: Node, plan: TestPlan): Unit = { FlightModel.step(node, plan); () }

  override def reportNames(node: Node): Set[String] = FlightModel.reportNames(node.get("flight"))

  override def build(node: Node, ctx: BuildContext): ChainBuilder = {
    val step = FlightModel.step(node, ctx.plan)
    val source = FlightModel.source(step.datasource, FlightModel.sourceNode(ctx.plan, step.datasource, node))
    // so SLAs like api.<name>.responseTime.p95 find it
    FlightModel.reportNames(node.get("flight")).foreach(n => ctx.paths.addRequest(ctx.groupPath, n))
    exec(new FlightActionBuilder(step, source))
  }
}

/** `type: flight` data sources: an Arrow Flight server that `flight` steps call. */
final class FlightDataSourcePlugin extends DataSourcePlugin {
  override val kind: String = "flight"

  override def validate(node: Node, plan: TestPlan): Unit = { FlightModel.source(node.path.split('.').last, node); () }
}
