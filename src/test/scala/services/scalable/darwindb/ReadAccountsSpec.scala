package services.scalable.darwindb

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.google.protobuf.any.Any
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.datalog.DefaultDatalogSerializers.grpcBlockSerializer
import services.scalable.datalog.grpc.{DBMeta, Datom}
import services.scalable.datalog.{CQLStorage, DatomDatabase}
import services.scalable.index.DefaultComparators.ord
import services.scalable.index.impl.DefaultCache
import services.scalable.index.Bytes

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.FutureConverters._

class ReadAccountsSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  def printDatom(d: Datom, p: String): String = {
    p match {
      case "users/:balance" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.tx},${d.tmp}]"
      case _ => ""
    }
  }

  "it " should "read accounts successfully" in {

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

    def readAllAccounts(): Future[Seq[DatomDatabase]] = {
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

    val accounts = Await.result(readAllAccounts(), Duration.Inf)

    val termOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        val r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

        if(r != 0) return r

        ord.compare(x.getA.getBytes(), y.getA.getBytes())
      }
    }

    def find(a: String, id: String, db: DatomDatabase): Future[Option[Datom]] = {
      val it = db.eavtIndex.find(Datom(e = Some(id), a = Some(a)), false, termOrd)
      Helper.one(it).map(_.headOption.map(_._1))
    }

    val balances = Await.result(Future.sequence(accounts.map{db => find("users/:balance", db.name, db)
      .map(d => d.map(d => d.getE -> java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt() -> d.getTx))}), Duration.Inf)

    logger.info(s"${Console.GREEN_B}balances: ${balances}${Console.RESET}\n")

    session.close()
  }

}
