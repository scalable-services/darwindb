package cluster

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.GrpcClientSettings
import cluster.protocol.{CoordinatorServiceClient, TaskRequest}
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class ClientSpec extends AnyFlatSpec with Repeatable {

  override val times = 1

  val logger = LoggerFactory.getLogger(this.getClass)
  val rand = ThreadLocalRandom.current()

  "amount of money " should " be the same after transactions" in {

    val session = CqlSession
      .builder()
      //.addContactPoint(new InetSocketAddress("localhost", 9042))
      .withConfigLoader(Config.loader)
      //.withLocalDatacenter(Config.DC)
      .withKeyspace(Config.KEYSPACE)
      .build()

    val READ_DATA  = session.prepare(s"select value, version from data where id=?;")

    def readData(k: String): Future[(String, Int)] = {
      session.executeAsync(READ_DATA.bind().setString(0, k)).toScala.map { rs =>
        val one = rs.one()
        one.getString("version") -> one.getInt("value")
      }
    }

    var clients = Map.empty[Int, CoordinatorServiceClient]
    val ports = (0 until Config.NUM_COORDINATORS).map{Config.COORDINATOR_BASE_PORT + _}

    val system = ActorSystem[Nothing](Behaviors.empty[Nothing], "Client", ConfigFactory.load("client.conf"))

    for(i<-0 until Config.NUM_COORDINATORS){

      implicit val classicActorSystem = system.classicSystem
      implicit val ec: ExecutionContext = system.executionContext

      val settings = GrpcClientSettings.connectToServiceAt("127.0.0.1", ports(rand.nextInt(0, ports.length)))
        .withTls(false)
      val client = CoordinatorServiceClient(settings)

      clients = clients + (i -> client)
    }

    def getAccounts(): Map[String, (String, Int)] = {
      var accounts = Map.empty[String, (String, Int)]

      val aresults = session.execute(s"select * from data;")

      aresults.forEach(r => {
        val key = r.getString("id")
        val version = r.getString("version")
        val value = r.getInt("value")

        accounts = accounts + (key -> (version, value))
      })

      accounts
    }

    val before = session.execute("select sum(value) as total from data;").one.getInt("total")

    val accounts = getAccounts()
    //val accountsBefore = accounts.map{case (k, (version, value)) => k -> (version, value)}

    var tasks = Seq.empty[Future[(String, Boolean, Long)]]
    val accSeq = accounts.keys.toSeq

    def submit(): Future[(String, Boolean, Long)] = {
      val client = clients(rand.nextInt(0, clients.size))

      val tid = UUID.randomUUID.toString()
      val keys = (0 until 2).map { _ => accSeq(rand.nextInt(0, accSeq.length)) }

      val k0 = keys(0)
      val k1 = keys(1)

      if(k0.compareTo(k1) == 0) return Future.successful(Tuple3(tid, true, 0L))

      Future.sequence(keys.map { k => readData(k).map(k -> _) }).flatMap { values =>

        val (k0, (vs0, v0)) = values(0)
        val (k1, (vs1, v1)) = values(1)

        // For distant datacenters this could improve the chance to be executed.
       // tid = smallest + "#" + tid

        var reads = Seq.empty[(String, String)]
        var writes = Seq.empty[(String, Int)]

        val ammount = if (v0 > 1) rand.nextInt(0, v0) else v0

        val w0 = v0 - ammount
        val w1 = v1 + ammount

        reads = reads ++ Seq(
          k0 -> vs0,
          k1 -> vs1
        )

        writes = writes ++ Seq(
          k0 -> w0,
          k1 -> w1
        )

        val workers = keys.map{k => computeWorker(computePartition(k))}.distinct.sorted

        val req = TaskRequest(tid, reads.toMap, writes.toMap, workers)

        val t0 = System.currentTimeMillis()
        client.submit(req).map { res =>
          val elapsed = System.currentTimeMillis() - t0
          Tuple3(res.id, res.succeed, elapsed)
        }
      }
    }

    val n = 100//rand.nextInt(1000, 2500)

    for(i<-0 until n){
      tasks = tasks :+ submit().map { case (tid, ok, elapsed) =>
        logger.info(s"${if(ok) Console.GREEN_B else Console.RED_B}tx ${tid} = $ok ${Console.RESET}")
        Tuple3(tid, ok, elapsed)
      }
    }

    val results = Await.result(Future.sequence(tasks), Duration.Inf)

    val success = results.count(_._2 == true)
    val len = results.length
    val elapsed = results.map(_._3).sum.toDouble

    val after = session.execute("select sum(value) as total from data;").one.getInt("total")

    logger.info(s"${Console.GREEN_B}\nbefore: ${before} after: ${after} success rate: ${(success*100/len)}% avg time: ${elapsed/len}ms${Console.RESET}\n")

    val closeAll = for {
      _ <- Future.sequence(clients.values.map{_.close()})
      _ <- session.closeAsync().toCompletableFuture.toScala
      //_ <- system.whenTerminated
    } yield {
      true
    }

    Await.ready(closeAll, Duration.Inf)

    system.terminate()

    assert(before == after)

  }

}
