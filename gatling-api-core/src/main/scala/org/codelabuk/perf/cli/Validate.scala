package org.codelabuk.perf.cli

import java.nio.file.Paths

import org.codelabuk.perf.data.{PlanData, Worker}
import org.codelabuk.perf.load.{LoadMath, PlanException, PlanLoader, RunOptions}
import org.codelabuk.perf.model.{DataSourceDef, TestPlan}

/**
 * Checks a plan without running it, and shows how it would be split across workers.
 *
 *   Validate <plan.yaml> [--workers N] [--scenario a,b]
 *
 * Exit code 0 = valid, 1 = invalid. Placeholders (${X}) must be resolvable, exactly as for a real run.
 */
object Validate {

  final case class Result(ok: Boolean, report: String)

  def main(args: Array[String]): Unit = {
    val r = run(args.toList)
    (if (r.ok) System.out else System.err).println(r.report)
    sys.exit(if (r.ok) 0 else 1)
  }

  def run(args: List[String]): Result = {
    def opt(name: String): Option[String] = args.sliding(2).collectFirst { case List(`name`, v) => v }
    val positional = args.zipWithIndex.collect { case (a, i) if !a.startsWith("--") && (i == 0 || !args(i - 1).startsWith("--")) => a }
    val planArg = positional.headOption.orElse(RunOptions.setting("plan", "PLAN"))
    planArg match {
      case None => Result(ok = false, "usage: Validate <plan.yaml> [--workers N] [--scenario a,b]")
      case Some(path) =>
        try {
          val workers = opt("--workers").map(w => w.toIntOption.filter(_ >= 1).getOrElse(throw new PlanException(s"--workers must be a positive number, got '$w'"))).getOrElse(1)
          val only = opt("--scenario").toList.flatMap(_.split(',')).map(_.trim).filter(_.nonEmpty).toSet
          val plan = PlanLoader.load(Paths.get(path))
          val unknown = only -- plan.scenarios.map(_.name)
          if (unknown.nonEmpty) throw new PlanException(s"--scenario: unknown scenario(s) ${unknown.mkString(", ")}")
          Result(ok = true, describe(plan, workers, only))
        } catch {
          case e: PlanException => Result(ok = false, s"INVALID: ${e.getMessage}")
        }
    }
  }

  private def describe(plan: TestPlan, workers: Int, only: Set[String]): String = {
    val scenarios = plan.scenarios.filter(s => only.isEmpty || only(s.name))
    val sb = new StringBuilder
    sb ++= s"VALID: plan '${plan.name}' - ${plan.apis.size} API(s), ${plan.scenarios.size} scenario(s), " +
      s"${plan.dataSources.size} data source(s), ${plan.sla.size} SLA(s)\n"
    sb ++= s"  target: ${plan.baseUrl}\n"
    plan.setup.foreach(s => sb ++= s"  setup: ${s.steps.size} step(s), shares: ${s.share.map(_.name).mkString(", ")} (sizes known only at run time)\n")

    // only data sources that feed rows to users (CSV, feeder queries) - not connections such as jdbc / flight servers
    val dataNames = PlanData.sources(plan, Worker(0, workers)).keys.toList.sorted
    val perWorker = (0 until workers).map { i =>
      val w = Worker(i, workers)
      val sources = PlanData.sources(plan, w)
      val load = scenarios.map(sc => LoadMath.describe(sc.load, w))
      val warnings = scenarios.flatMap { sc =>
        LoadMath.plannedArrivals(sc.load, w).toList.flatMap { arrivals =>
          sc.feeders.flatMap(sources.get).filter(s => s.strategy == "queue" && s.size < arrivals)
            .map(s => s"'${sc.name}' needs ~$arrivals but '${s.name}' has ${s.size}")
        }
      }
      (i, load, dataNames.map(n => sources(n).size), warnings)
    }

    val header = "worker" :: scenarios.map(s => s"scenario ${s.name}") ++ dataNames.map(d => s"data $d")
    val rows = perWorker.map { case (i, load, sizes, _) => s"${i + 1}/$workers" :: load ++ sizes.map(_.toString) }
    val widths = header.indices.map(c => (header :: rows.toList).map(r => r(c).length).max)
    def line(r: List[String]) = "  " + r.zip(widths).map { case (v, w) => v.padTo(w, ' ') }.mkString("  ")
    sb ++= s"\n  Split across $workers worker(s):\n"
    sb ++= line(header) + "\n"
    rows.foreach(r => sb ++= line(r) + "\n")

    val splitOff = plan.dataSources.values.collect { case c: DataSourceDef.Csv if !c.splitAcrossWorkers => c.name }
    if (workers > 1 && splitOff.nonEmpty) sb ++= s"\n  note: ${splitOff.mkString(", ")} not split - every worker uses all rows\n"
    val warnings = perWorker.flatMap { case (i, _, _, ws) => ws.map(w => s"worker ${i + 1}: $w") }
    if (warnings.nonEmpty) {
      sb ++= "\n  WARNINGS (queue data runs out - extra users stop immediately):\n"
      warnings.foreach(w => sb ++= s"    $w\n")
    }
    val throttleWarnings = scenarios.flatMap(LoadMath.throttleWarning)
    if (throttleWarnings.nonEmpty) {
      sb ++= "\n  WARNINGS (throttle):\n"
      throttleWarnings.foreach(w => sb ++= s"    $w\n")
    }
    sb.result()
  }
}
