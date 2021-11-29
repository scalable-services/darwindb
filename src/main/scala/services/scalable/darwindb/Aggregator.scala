package services.scalable.darwindb

import akka.actor.ActorSystem
import akka.actor.typed.{Behavior, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.Source
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.sksamuel.pulsar4s.akka.streams.sink
import com.sksamuel.pulsar4s.{MessageId, ProducerConfig, ProducerMessage, PulsarClient, PulsarClientConfig, Topic}
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.Schema
import services.scalable.darwindb.protocol.Position

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration.DurationLong
import scala.language.postfixOps

object Aggregator {

  trait Command extends CborSerializable

  final case object Stop extends Command
  final case object BatchTaskTick extends Command

  def apply(): Behavior[Command] = Behaviors.setup[Command] { ctx =>

    val logger = ctx.log

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

    val offsets = TrieMap.empty[Int, MessageId]

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

    var batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)

    def task(): Unit = {

      for(i<-0 until Config.NUM_LOG_PARTITIONS){

        val p = s"persistent://public/darwindb/log-partition-${i}"

        val mid = topicsAdmin.getLastMessageId(p)
        val offset = offsets(i)

        if(mid.compareTo(offset) > 0){
          logger.info(s"${Console.MAGENTA_B}log-partition-$i has changed: ${offset} => ${mid}${Console.RESET}")

          offsets.update(i, mid)

          val record = ProducerMessage[Array[Byte]](Any.pack(Position(p, ByteString.copyFrom(offset.toByteArray), ByteString.copyFrom(mid.toByteArray), i)).toByteArray)
          Source.single(record).to(sink(producer)).run()
        }
      }

      batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)
    }

    def close(): Future[Unit] = {

      client.close()
      batchTask.cancel()

      Future.successful{}
    }

    logger.info(s"\n${Console.BLUE_B}STARTING AGGREGATOR...${Console.RESET}\n")

    Behaviors.receiveMessage[Command] {
      case BatchTaskTick =>
        task()
        Behaviors.same

      case Stop => Behaviors.stopped
      case _ => Behaviors.same
    }.receiveSignal {
      case (context, PostStop) =>
        close()
        Behaviors.same

      case _ => Behaviors.same
    }
  }

}
