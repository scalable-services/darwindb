package services.scalable.darwindb

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.GrpcClientSettings
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.google.protobuf.any.Any
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.darwindb.protocol.{CoordinatorServiceClient, TaskRequest}
import services.scalable.datalog.DefaultDatalogSerializers.grpcBlockSerializer
import services.scalable.index.DefaultComparators.ord
import services.scalable.datalog.grpc.{DBMeta, Datom}
import services.scalable.datalog.{CQLStorage, DatomDatabase}
import services.scalable.index.Bytes
import services.scalable.index.impl.DefaultCache

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.FutureConverters._
import scala.language.postfixOps

class ClientDBSpec extends AnyFlatSpec with Repeatable {

  override val times = 1

  val logger = LoggerFactory.getLogger(this.getClass)
  val rand = ThreadLocalRandom.current()

  "amount of money " should " be the same after transactions" in {

    val session = CqlSession
      .builder()
      .withConfigLoader(Config.loader)
      .withKeyspace(Config.DB_KEYSPACE)
      .build()

    val READ_ACCOUNTS = session.prepare(s"select * from meta;")

    val NUM_LEAF_ENTRIES = 64
    val NUM_META_ENTRIES = 64

    val EMPTY_ARRAY = Array.empty[Byte]

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new CQLStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES, session)

    def readAllDBs(): Future[Seq[DatomDatabase]] = {
      var list = Seq.empty[DatomDatabase]

      def next(it: AsyncResultSet): Future[Seq[DatomDatabase]] = {
        val page = it.currentPage()

        page.forEach { r =>
          val name = r.getString("name")
          val roots = Any.parseFrom(r.getByteBuffer("roots").array()).unpack(DBMeta)

          val db = new DatomDatabase(name, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(global, session, grpcBlockSerializer, cache, storage)
          db.loadFromMeta(roots)

          list = list :+ db
        }

        if(it.hasMorePages){
          it.fetchNextPage().asScala.flatMap(next(_))
        } else {
          Future.successful(list)
        }
      }

      session.executeAsync(READ_ACCOUNTS.bind()).asScala.flatMap(next(_))
    }

    val termOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        val r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

        if(r != 0) return r

        ord.compare(x.getA.getBytes(), y.getA.getBytes())
      }
    }

    def find(a: String, id: String, db: DatomDatabase, reload: Boolean): Future[Option[Datom]] = {
      if(reload) {
        return db.load().flatMap { _ =>
          val it = db.eavtIndex.find(Datom(e = Some(id), a = Some(a)), false, termOrd)
          Helper.one(it).map(_.headOption.map(_._1))
        }
      }

      val it = db.eavtIndex.find(Datom(e = Some(id), a = Some(a)), false, termOrd)
      Helper.one(it).map(_.headOption.map(_._1))
    }

    val dbs = Await.result(readAllDBs(), Duration.Inf)
    val dbMap = dbs.map{d => d.name -> d}.toMap

    def getAccounts(): Future[Map[String, (String, Int)]] = {
      var accounts = Map.empty[String, (String, Int)]

      Future.sequence(dbs.map{db => find("users/:balance", db.name, db, false)}).map { datoms =>
        datoms.filter(_.isDefined).map(_.get).map{ d =>
          val id = d.getE
          val value = java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()
          val version = d.getTx

          accounts = accounts + (id -> (version, value))
        }

        accounts
      }
    }

    var clients = Map.empty[Int, CoordinatorServiceClient]
    val ports = (0 until Config.NUM_COORDINATORS).map{3551 + _}

    val system = ActorSystem[Nothing](Behaviors.empty[Nothing], "Client", ConfigFactory.load("client.conf"))

    for(i<-0 until Config.NUM_COORDINATORS){

      implicit val classicActorSystem = system.classicSystem
      implicit val ec: ExecutionContext = system.executionContext

      val settings = GrpcClientSettings.connectToServiceAt("127.0.0.1", ports(rand.nextInt(0, ports.length)))
        .withTls(false)
      val client = CoordinatorServiceClient(settings)

      clients = clients + (i -> client)
    }

    val accounts = Await.result(getAccounts(), Duration.Inf)

    def getMoney(reload: Boolean = false): Future[Seq[(String, Int)]] = {
      Future.sequence(dbs.map{db => find("users/:balance", db.name, db, reload)
        .map(d => d.map(d => d.getE -> java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()))
        .filter(_.isDefined).map(_.get)
      })
    }

    val accountsMoneyBefore = Await.result(getMoney(), Duration.Inf).toMap
    val before = accountsMoneyBefore.map(_._2).sum

    def readData(id: String, reload: Boolean): Future[(String, Int)] = {
      val db = dbMap(id)

      find("users/:balance", db.name, db, reload).map(_.get).map { d =>
        d.getTx -> java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()
      }
    }

    var tasks = Seq.empty[Future[(String, Boolean, Long)]]
    val accSeq = accounts.keys.toSeq

    def submit(): Future[(String, Boolean, Long)] = {
      val client = clients(rand.nextInt(0, clients.size))

      val tid = UUID.randomUUID.toString()
      val keys = (0 until 2).map { _ => accSeq(rand.nextInt(0, accSeq.length)) }

      val k0 = keys(0)
      val k1 = keys(1)

      if(k0.compareTo(k1) == 0) return Future.successful(Tuple3(tid, true, 0L))

      Future.sequence(keys.map { k => readData(k, true).map(k -> _) }).flatMap { values =>

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

    val accountsMoneyAfter = Await.result(getMoney(true), Duration.Inf).toMap
    val after = accountsMoneyAfter.map(_._2).sum

    //logger.info(s"${Console.MAGENTA_B}accounts money before: ${accountsMoneyBefore}${Console.RESET}\n")

    //logger.info(s"${Console.MAGENTA_B}accounts money after: ${accountsMoneyAfter}${Console.RESET}\n")
    logger.info(s"${Console.RED_B}accounts changed: ${accountsMoneyAfter.filter{case (id, after) => accountsMoneyBefore(id) != after}
      .map{ case (id, after) =>
        id -> (accountsMoneyBefore(id), after)
      }.mkString("\n")}${Console.RESET}")

    logger.info(s"${Console.GREEN_B}\nbefore: ${before} after: ${after} success rate: ${(success*100/len)}% avg time: ${elapsed/len}ms${Console.RESET}\n")

    val closeAll = for {
      _ <- Future.sequence(clients.values.map{_.close()})
      _ <- session.closeAsync().asScala
    } yield {
      true
    }

    Await.ready(closeAll, Duration.Inf)

    system.terminate()

    assert(before == after)

  }

}
