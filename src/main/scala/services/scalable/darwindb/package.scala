package services.scalable

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.google.common.hash.Hashing
import services.scalable.index.{AsyncIterator, Tuple}

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.hashing.MurmurHash3

package object darwindb {

  object Config {

    val loader =
      DriverConfigLoader.programmaticBuilder()
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30))
        .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
        .withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, 1000)
        .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
        .withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, "ExponentialReconnectionPolicy")
        .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, java.time.Duration.ofSeconds(1))
        .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, java.time.Duration.ofSeconds(10))
        /*.startProfile("slow")
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
        .endProfile()*/
        .build()

    val KEYSPACE = "scheduler"
    val DB_KEYSPACE = "indexes"

    val NUM_WORKERS = 3
    val NUM_COORDINATORS = 3

    val COORDINATOR_BASE_PORT = 3551

    val PULSAR_SERVICE_URL = "pulsar://localhost:6650"
    val PULSAR_CLIENT_URL = "http://localhost:8080"

    object Topics {
      val LOG = "persistent://public/darwindb/log"
      val TOPOLOGY_LOG = "persistent://public/darwindb/topology"
    }

    var topics = Seq.empty[String]

    for(i<-0 until Config.NUM_COORDINATORS){
      topics = topics :+ s"persistent://public/darwindb/coord-${i}"
    }

    for(i<-0 until Config.NUM_WORKERS){
      topics = topics :+ s"persistent://public/darwindb/worker-${i}"
    }

    val NUM_LOG_PARTITIONS = 3
    val NUM_DATA_PARTITIONS = Int.MaxValue

    val COORDINATOR_INTERVAL = 10L
  }

  val WORKERS = (0 until Config.NUM_WORKERS).map { i =>
    s"worker-${i}"
  }

  val COORDINATORS = (0 until Config.NUM_COORDINATORS).map { i =>
    s"coordinator-${i}"
  }

  def computePartition(key: String): Int = {
    (MurmurHash3.stringHash(key).abs % Config.NUM_DATA_PARTITIONS).abs
  }

  def computeWorker(dp: Int): Int = {
    (dp % Config.NUM_WORKERS).abs
  }

  def computeCoordinator(id: String): Int = {
    (Hashing.murmur3_128().hashBytes(id.getBytes()).asInt() % Config.NUM_COORDINATORS).abs
  }

  object Helper {
    def all[K, V](it: AsyncIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          all(it).map{list ++ _}
        }
        case false => Future.successful(Seq.empty[Tuple[K, V]])
      }
    }

    def one[K, V](it: AsyncIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Option[Tuple[K, V]]] = {
      it.hasNext().flatMap {
        case true => it.next().map { list =>
          list.headOption
        }
        case false => Future.successful(None)
      }
    }
  }
}
