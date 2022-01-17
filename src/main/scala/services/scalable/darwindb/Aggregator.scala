package services.scalable.darwindb

import akka.actor.{ActorSystem, Cancellable}
import akka.actor.typed.{Behavior, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.Source
import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.sksamuel.pulsar4s.akka.streams.sink
import com.sksamuel.pulsar4s.{MessageId, ProducerConfig, ProducerMessage, PulsarClient, PulsarClientConfig, Topic}
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.Schema
import services.scalable.darwindb.protocol.{Offsets, Position}

import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration.DurationLong
import scala.jdk.FutureConverters._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * TODO Implement the logic for saving the state of the aggregator before failure.
 */
object Aggregator {

  trait Command extends CborSerializable

  final case object Stop extends Command
  final case object BatchTaskTick extends Command

  def apply(id: String): Behavior[Command] = Behaviors.setup[Command] { ctx =>

    val logger = ctx.log
    implicit val ec = ctx.executionContext

    val session = CqlSession
      .builder()
      //.addContactPoint(new InetSocketAddress(Config.CASSANDRA_HOST, Config.CASSANDRA_PORT))
      .withConfigLoader(Config.loader)
      //.withLocalDatacenter(Config.DC)
      .withKeyspace(Config.KEYSPACE)
      .build()

    val offsets = TrieMap.empty[Int, MessageId]

    val UPDATE_STATE = session.prepare("update aggregators set offsets = :offsets where id = :id;")
    val GET_STATE = session.prepare("select * from aggregators where id = :id;")

    def setState(): Future[Boolean] = {
      session.executeAsync(GET_STATE.bind().setString("id", id)).asScala.map { rs =>
        val one = rs.one()

        if(one == null){
          false
        } else {

          val bytes = one.getByteBuffer("offsets").array()
          val o = Offsets.parseFrom(bytes)

          o.offsets.foreach { case (p, b) =>
            offsets.put(p, org.apache.pulsar.client.api.MessageId.fromByteArray(b.toByteArray))
          }

          true
        }
      }
    }

    def updateState(): Future[Boolean] = {
      val o = Offsets(id, offsets.map {case (p, m) => p -> ByteString.copyFrom(m.toByteArray)}.toMap)

      session.executeAsync(UPDATE_STATE.bind()
      .setByteBuffer("offsets", ByteBuffer.wrap(Any.pack(o).toByteArray))
        .setString("id", id)
      ).asScala.map(_.wasApplied())
    }

    // Pass auth-plugin class fully-qualified name if Pulsar-security enabled
    val authPluginClassName = "pulsar"
    // Pass auth-param if auth-plugin class requires it
    val authParams = "param1=value1"
    val useTls = false
    val tlsAllowInsecureConnection = true
    val tlsTrustCertsFilePath = null
    val admin = PulsarAdmin.builder()
      //authentication(authPluginClassName, authParams)
      .serviceHttpUrl(Config.PULSAR_CLIENT_URL)
      .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
      .allowTlsInsecureConnection(tlsAllowInsecureConnection).build()

    val topicsAdmin = admin.topics()

    for(i<-0 until Config.NUM_LOG_PARTITIONS){
      val p = s"persistent://public/darwindb/log-partition-${i}"
      offsets.put(i, topicsAdmin.getLastMessageId(p))
    }

    implicit val classicSystem: ActorSystem = ctx.system.classicSystem

    val config = PulsarClientConfig(serviceUrl = Config.PULSAR_SERVICE_URL, allowTlsInsecureConnection = Some(true))
    val client = PulsarClient(Config.PULSAR_SERVICE_URL)
    implicit val schema: Schema[Array[Byte]] = Schema.BYTES

    val producer = () => client.producer[Array[Byte]](ProducerConfig(topic =
      Topic(Config.Topics.TOPOLOGY_LOG), enableBatching = Some(false), blockIfQueueFull = Some(false)))

    var batchTask: Cancellable = null

    /*setState().onComplete {
      case Success(ok) =>

        logger.info(s"\n${Console.BLUE_B}$id offsets: ${offsets}${Console.RESET}\n")
        batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)

      case Failure(ex) => logger.error(ex.toString)
    }*/

    logger.info(s"\n${Console.BLUE_B}$id offsets: ${offsets}${Console.RESET}\n")
    batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)

    def task(): Unit = {

      var changed = false

      for(i<-0 until Config.NUM_LOG_PARTITIONS){

        val p = s"persistent://public/darwindb/log-partition-${i}"

        val mid = topicsAdmin.getLastMessageId(p)
        val offset = offsets(i)

        if(mid.compareTo(offset) > 0){
          changed = true

          logger.info(s"${Console.MAGENTA_B}log-partition-$i has changed: ${offset} => ${mid}${Console.RESET}")

          offsets.update(i, mid)

          val record = ProducerMessage[Array[Byte]](Any.pack(Position(p, ByteString.copyFrom(offset.toByteArray), ByteString.copyFrom(mid.toByteArray), i)).toByteArray)
          Source.single(record).to(sink(producer)).run()
        }
      }

      /*updateState().onComplete {
        case Success(ok) =>
          batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)

        case Failure(ex) => logger.error(ex.toString)
      }*/

      batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)
    }

    def close(): Future[Unit] = {

      client.close()
      if(batchTask != null) batchTask.cancel()

      Future.successful{}
    }

    logger.info(s"\n${Console.BLUE_B}STARTING AGGREGATOR...${Console.RESET}\n")

    Behaviors.receiveMessage[Command] {
      case BatchTaskTick =>
        task()
        Behaviors.same

      case Stop =>
        close()
        Behaviors.stopped

      case _ => Behaviors.same
    }/*.receiveSignal {

      case (context, PostStop) =>
        close()
        Behaviors.same

      case _ => Behaviors.same
    }*/
  }

}
