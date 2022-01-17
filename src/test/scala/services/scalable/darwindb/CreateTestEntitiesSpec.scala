package services.scalable.darwindb

import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.datalog.{CQLStorage, DatomDatabase}
import services.scalable.datalog.DefaultDatalogSerializers._
import services.scalable.datalog.grpc.Datom
import services.scalable.index.DefaultComparators.ord
import services.scalable.index.impl.DefaultCache
import services.scalable.index.{Bytes, loader}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CreateTestEntitiesSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  def printDatom(d: Datom, p: String): String = {
    p match {
      case "users/:balance" => s"[${d.a},${services.scalable.datalog.Helper.readInt(d.getV.toByteArray)},${d.e},${d.tx},${d.tmp}]"
      case _ => ""
    }
  }

  "it " should "transact correctly" in {

    val session = CqlSession
      .builder()
      .withConfigLoader(loader)
      .withKeyspace(Config.DB_KEYSPACE)
      .build()

    val rand = ThreadLocalRandom.current()

    val NUM_LEAF_ENTRIES = 64
    val NUM_META_ENTRIES = 64

    val EMPTY_ARRAY = Array.empty[Byte]

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new CQLStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES, session)

    var accounts = Seq.empty[DatomDatabase]

    val n = 1000

    var tasks = Seq.empty[Future[Boolean]]

    val tx = UUID.randomUUID().toString

    for(i<-0 until n){
      val id = UUID.randomUUID().toString
      val db = new DatomDatabase(id, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(global, session, grpcBlockSerializer, cache, storage)

      accounts = accounts :+ db

      val balance = java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(0, 1000)).flip().array()

      tasks :+= (for {
        _ <- db.loadOrCreate()
        _ <- db.insert(Seq(
          Datom(
            e = Some(id),
            a = Some("users/:balance"),
            v = Some(ByteString.copyFrom(balance)),
            tx = Some(tx),
            tmp = Some(System.currentTimeMillis())
          ) -> EMPTY_ARRAY
        ))
        ok <- db.save()
      } yield {
        ok
      })
    }

    val results = Await.result(Future.sequence(tasks), Duration.Inf)

    logger.info(s"${Console.GREEN_B}results: ${results}${Console.RESET}\n")

    val termOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        var r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

        if(r != 0) return r

        ord.compare(x.getA.getBytes(), y.getA.getBytes())
      }
    }

    def find(a: String, id: String, db: DatomDatabase): Future[Option[Datom]] = {
      val it = db.eavtIndex.find(Datom(e = Some(id), a = Some(a)), false, termOrd)
      Helper.one(it).map(_.headOption.map(_._1))
    }

    val balances = Await.result(Future.sequence(accounts.map{db => find("users/:balance", db.name, db)}), Duration.Inf)

    logger.info(s"${Console.GREEN_B}balances: ${balances.filter(_.isDefined).map(_.get).map{ d =>
      val b = java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()
      d.getE -> b
    }}${Console.RESET}\n")

    session.close()
  }

}
