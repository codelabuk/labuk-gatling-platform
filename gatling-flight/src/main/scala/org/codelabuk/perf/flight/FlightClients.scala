package org.codelabuk.perf.flight

import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import com.typesafe.scalalogging.StrictLogging
import org.apache.arrow.flight.{CallOption, CallOptions, FlightCallHeaders, FlightClient, HeaderCallOption, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

/**
 * Per Flight data source, per JVM: an allocator, `connections` clients (gRPC channels, used round-robin), a pool of
 * `size` threads that run the calls (Flight's Java client blocks - never on Gatling's threads) and a pool for
 * fetching several endpoints of one query in parallel.
 */
final class FlightPool(val source: FlightSource) extends StrictLogging {
  val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)
  private val next = new AtomicInteger
  private val clients: Vector[FlightClient] = Vector.fill(source.connections)(open(source.uri))
  private val others = new ConcurrentHashMap[String, FlightClient]() // endpoints served from another location

  val executor: ExecutorService = Executors.newFixedThreadPool(source.poolSize, threads(s"flight-${source.name}"))
  val fetchers: ExecutorService = Executors.newCachedThreadPool(threads(s"flight-${source.name}-fetch"))

  /** Call options for every call: the timeout and the data source's headers (e.g. authorization). */
  val options: Array[CallOption] = {
    val timeout: CallOption = CallOptions.timeout(source.timeout.toMillis, TimeUnit.MILLISECONDS)
    if (source.headers.isEmpty) Array(timeout)
    else {
      val h = new FlightCallHeaders()
      source.headers.foreach { case (k, v) => h.insert(k, v) }
      Array(timeout, new HeaderCallOption(h))
    }
  }

  def client(): FlightClient = clients(Math.floorMod(next.getAndIncrement(), clients.size))

  /** The client for an endpoint: ours unless the endpoint names another server. */
  def clientFor(locations: java.util.List[Location]): FlightClient =
    locations.asScala.map(_.getUri).find(u => !sameServer(u)) match {
      case None      => client()
      case Some(uri) => others.computeIfAbsent(uri.toString, _ => open(uri.toString))
    }

  private def sameServer(u: URI): Boolean =
    u.getScheme == "arrow-flight-reuse-connection" || u.toString.stripSuffix("/") == source.uri.stripSuffix("/")

  private def open(uri: String): FlightClient = {
    val b = FlightClient.builder(allocator, new Location(uri))
    if (!source.verifyServer) b.verifyServer(false)
    b.build()
  }

  private def threads(prefix: String): ThreadFactory = {
    val counter = new AtomicInteger
    r => {
      val t = new Thread(r, s"$prefix-${counter.incrementAndGet()}")
      t.setDaemon(true)
      t
    }
  }

  def close(): Unit = {
    executor.shutdownNow()
    fetchers.shutdownNow()
    (clients ++ others.values().asScala).foreach(c => try c.close() catch { case _: Exception => () })
    try allocator.close() catch { case _: Exception => () } // buffers still held by aborted calls - process is ending
  }
}

object FlightPools extends StrictLogging {
  private val pools = new ConcurrentHashMap[String, FlightPool]()

  Runtime.getRuntime.addShutdownHook(new Thread(() => closeAll(), "flight-pools-shutdown"))

  def get(source: FlightSource): FlightPool = pools.computeIfAbsent(source.name, _ => {
    logger.info(s"flight '${source.name}': ${source.connections} connection(s), ${source.poolSize} call thread(s) to ${source.uri}")
    new FlightPool(source)
  })

  def closeAll(): Unit = {
    pools.values().forEach(_.close())
    pools.clear()
  }
}
