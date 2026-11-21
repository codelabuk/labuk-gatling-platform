package org.codelabuk.perf.flight

import java.util.concurrent.RejectedExecutionException

import scala.util.control.NonFatal

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.session.el._
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext

final class FlightActionBuilder(step: FlightStep, source: FlightSource) extends ActionBuilder {
  override def build(ctx: ScenarioContext, next: Action): Action =
    new FlightAction(step, FlightPools.get(source), ctx.coreComponents.statsEngine, ctx.coreComponents.clock, next)
}

/**
 * Runs one `flight` step for a virtual user:
 *  1. resolves #{...} in the command (and the rows of a put) on the Gatling thread,
 *  2. runs the call on the data source's own thread pool (Flight's Java client blocks - never on Gatling's threads),
 *  3. records it as a request named after the step (plus "(plan)" / "(first batch)" with `phases: true`),
 *     then resumes the user on its own event loop.
 */
final class FlightAction(step: FlightStep, pool: FlightPool, val statsEngine: StatsEngine, clock: Clock, val next: Action)
    extends ChainableAction {

  override val name: String = s"flight-${step.name}"

  private val command: Expression[String] = step.mode match {
    case q: FlightMode.Query => q.command.el[String]
    case p: FlightMode.Put   => p.command.el[String]
  }
  private val schemaCommand: Expression[Option[String]] = step.mode match {
    case FlightMode.Put(_, _, _, None, Some(cmd), _) => cmd.el[String].map(Some(_))
    case _                                            => _ => None.success
  }

  /** The rows of a put: literal rows (values may be #{...}), repeated `times`, or a session list of maps. */
  private val putRows: Session => Validation[Seq[Map[String, Any]]] = step.mode match {
    case FlightMode.Put(_, PutRows.Literal(rows, times), _, _, _, _) =>
      val exprs = rows.map(_.map {
        case (col, Left(el))  => col -> el.el[Any]
        case (col, Right(v))  => col -> ((_: Session) => v.success)
      })
      def once(session: Session): Validation[List[Map[String, Any]]] =
        exprs.foldLeft[Validation[List[Map[String, Any]]]](Nil.success) { (acc, row) =>
          acc.flatMap(done => row.foldLeft[Validation[Map[String, Any]]](Map.empty[String, Any].success) { case (r, (c, e)) =>
            r.flatMap(m => e(session).map(v => m + (c -> v)))
          }.map(done :+ _))
        }
      // resolved again for every repetition, so #{randomUuid()} etc. give a new value in each copy
      session => (1 to times).foldLeft[Validation[Vector[Map[String, Any]]]](Vector.empty.success) { (acc, _) =>
        acc.flatMap(done => once(session).map(done ++ _))
      }
    case FlightMode.Put(_, PutRows.FromVariable(el), _, _, _, _) =>
      val list = s"#{$el}".el[Any]
      session => list(session).flatMap {
        case items: Iterable[_] =>
          items.toSeq.foldLeft[Validation[Vector[Map[String, Any]]]](Vector.empty.success) {
            case (acc, m: scala.collection.Map[_, _]) => acc.map(_ :+ m.asInstanceOf[scala.collection.Map[String, Any]].toMap)
            case (acc, m: java.util.Map[_, _]) =>
              import scala.jdk.CollectionConverters._
              acc.map(_ :+ m.asInstanceOf[java.util.Map[String, Any]].asScala.toMap)
            case (_, other) => s"put rows: items of '$el' must be maps of column -> value (got ${other.getClass.getSimpleName})".failure
          }
        case other => s"put rows: '$el' is not a list (got ${other.getClass.getSimpleName})".failure
      }
    case _ => _ => Nil.success
  }

  override protected def execute(session: Session): Unit = {
    val start = clock.nowMillis
    val resolved = for {
      cmd <- command(session)
      schemaCmd <- schemaCommand(session)
      rows <- putRows(session)
    } yield (cmd, schemaCmd, rows)

    resolved match {
      case Failure(err) => finish(session, start, Left(s"command: $err"))
      case Success((cmd, schemaCmd, rows)) =>
        try {
          pool.executor.execute { () =>
            val result =
              try step.mode match {
                case q: FlightMode.Query => FlightExecutor.query(pool, step, q, cmd, () => clock.nowMillis)
                case p: FlightMode.Put   => FlightExecutor.put(pool, step, p, cmd, schemaCmd, rows)
              } catch { case NonFatal(e) => Left(s"${e.getClass.getSimpleName}: ${e.getMessage}") }
            finish(session, start, result)
          }
        } catch {
          case _: RejectedExecutionException => finish(session, start, Left("flight pool is shut down"))
        }
    }
  }

  private def finish(session: Session, start: Long, result: Either[String, FlightOutcome]): Unit = {
    val end = clock.nowMillis
    val next2 = result match {
      case Right(out) =>
        if (step.phases) {
          out.planEnd.foreach(p => statsEngine.logResponse(session.scenario, session.groups, s"${step.name} (plan)", start, p, OK, None, None))
          out.firstBatch.foreach(f => statsEngine.logResponse(session.scenario, session.groups, s"${step.name} (first batch)", start, f, OK, None, None))
        }
        statsEngine.logResponse(session.scenario, session.groups, step.name, start, end, OK, None, None)
        session.setAll(out.saved)
      case Left(err) =>
        statsEngine.logResponse(session.scenario, session.groups, step.name, start, end, KO, None, Some(err))
        session.markAsFailed
    }
    // back onto the virtual user's own event loop before continuing its scenario
    session.eventLoop.execute(() => next ! next2)
  }
}
