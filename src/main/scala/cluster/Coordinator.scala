package cluster

import akka.Done
import akka.actor.ActorSystem
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, PostStop, SupervisorStrategy}
import akka.pattern.pipe
import akka.stream.{DelayOverflowStrategy, KillSwitches}
import akka.stream.scaladsl.{Sink, Source}
import cluster.protocol.{Batch, BatchDone, TaskRequest, TaskResponse}
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType}
import com.datastax.oss.driver.api.core.{CqlSession, DefaultConsistencyLevel}
import com.google.protobuf.any.Any
import com.sksamuel.pulsar4s.akka.streams.{sink, source}
import com.sksamuel.pulsar4s.{ConsumerConfig, ConsumerMessage, MessageId, Producer, ProducerConfig, ProducerMessage, PulsarClient, PulsarClientConfig, Subscription, Topic}
import io.netty.util.internal.ThreadLocalRandom
import org.apache.pulsar.client.api.{Schema, SubscriptionInitialPosition, SubscriptionType}

import java.nio.ByteBuffer
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.DurationLong
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.language.postfixOps

object Coordinator {

  trait Command extends CborSerializable

  final case object Stop extends Command
  final case object BatchTaskTick extends Command

  case class Request(request: TaskRequest, sender: ActorRef[TaskResponse]) extends Command

  def apply(name: String, id: Int): Behavior[Command] = Behaviors.supervise {
    Behaviors.setup[Command] { ctx =>

      implicit val ec = ctx.executionContext

      val logger = ctx.log
      val rand = ThreadLocalRandom.current()

      val session = CqlSession
        .builder()
        //.addContactPoint(new InetSocketAddress(Config.CASSANDRA_HOST, Config.CASSANDRA_PORT))
        .withConfigLoader(Config.loader)
        //.withLocalDatacenter(Config.DC)
        .withKeyspace(Config.KEYSPACE)
        .build()

      val INSERT_COMMAND = session.prepare("insert into commands(command_id, batch_id, data, completed) values(?, ?, ?, false);")
      val INSERT_META_BATCH = session.prepare("insert into batches(id, workers, completed, votes) values(?, ?, false, {});")

      val UPDATE_TOPIC_META = session.prepare(s"update topic_offsets set last = ?, offset = ? where topic = ? and partition = ?;")

      val queue = TrieMap.empty[String, (TaskRequest, Promise[TaskResponse])]
      val processing = TrieMap.empty[String, (TaskRequest, Promise[TaskResponse])]

      implicit val classicSystem: ActorSystem = ctx.system.classicSystem

      val config = PulsarClientConfig(serviceUrl = Config.PULSAR_SERVICE_URL, allowTlsInsecureConnection = Some(true))
      val client = PulsarClient(Config.PULSAR_SERVICE_URL)
      implicit val schema: Schema[Array[Byte]] = Schema.BYTES

      var partitions = Map.empty[Int, () => Producer[Array[Byte]]]

      for(i<-0 until Config.NUM_COORDINATORS){
        val producer = () => client.producer[Array[Byte]](ProducerConfig(topic =
          Topic(s"persistent://public/darwindb/log-partition-${i}"), blockIfQueueFull = Some(false)))
        partitions = partitions + Tuple2(i, producer)
      }

      def saveBatch(b: Batch): Future[Boolean] = {
        val stm = BatchStatement.builder(BatchType.LOGGED)

        stm.addStatement(INSERT_META_BATCH.bind().setString(0, b.id).setSet(1, b.workers.map(_.toString).toSet.asJava, classOf[String]))

        b.tasks.foreach { t =>
          stm.addStatement(INSERT_COMMAND.bind().setString(0, t.id).setString(1, b.id)
            .setByteBuffer(2, ByteBuffer.wrap(Any.pack(t).toByteArray)))
        }

        session.executeAsync(stm.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM).build()).asScala.map(_.wasApplied())
      }

      def log(b: Batch): Unit = {
        val producer = partitions(rand.nextInt(0, partitions.size))
        val buf = Any.pack(b).toByteArray
        val record = ProducerMessage[Array[Byte]](buf)

        Source.single(record).to(sink(producer)).run()
      }

      var batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)

      ctx.system.receptionist ! Receptionist.Register(ServiceKey[Coordinator.Command](name), ctx.self)

      def task(): Unit = {
        val commands = queue.map(_._2)

        if(commands.isEmpty){
          batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)
          return
        }

        var keys = Seq.empty[String]
        val it = commands.iterator
        var tasks = Seq.empty[(TaskRequest, Promise[TaskResponse])]

        val batchId = UUID.randomUUID.toString()

        var n = 0

        while(it.hasNext /*&& n < Config.MAX_COORDINATOR_BUFFER_SIZE*/){
          val next = it.next()
          val (cmd, p) = next

          if(!cmd.writes.keys.exists{keys.contains(_)}){
            keys = (keys ++ cmd.reads.keys.toSeq.distinct)
            tasks = tasks :+ cmd.withBatchId(batchId) -> p

            n += 1

          } else {
            p.success(TaskResponse(cmd.id, false))
            queue.remove(cmd.id)
          }
        }

        if(tasks.isEmpty){
          batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)
          return
        }

        val partitions = keys.map{computePartition(_)}.distinct
        val schedulers = partitions.map{computeWorker(_)}.distinct
        val scheduler = schedulers(rand.nextInt(0, schedulers.length))

        val batch = Batch(batchId, tasks.map(_._1), id, schedulers, scheduler, partitions)

        tasks.foreach { case (c, p) =>
          processing.put(c.id, c -> p)
        }

        logger.info(s"${Console.CYAN_B} $name CREATED BATCH ${batch.id} with schedulers with len ${batch.tasks.length} ${schedulers.sorted} with leader ${scheduler}${Console.RESET}")

        log(batch)

        tasks.foreach{case (cmd, _) => queue.remove(cmd.id)}

        batchTask = ctx.scheduleOnce[BatchTaskTick.type](Config.COORDINATOR_INTERVAL milliseconds, ctx.self, BatchTaskTick)
      }

      def handler(rec: ConsumerMessage[Array[Byte]]): Future[Boolean] = {
        val done = Any.parseFrom(rec.value).unpack(BatchDone)

        done.succeed.foreach { cid =>
          processing.remove(cid) match {
            case Some((c, p)) => p.success(TaskResponse(cid, true))
            case None =>
          }
        }

        done.failed.foreach { cid =>
          processing.remove(cid) match {
            case Some((c, p)) => p.success(TaskResponse(cid, false))
            case None =>
          }
        }

        Future.successful(true)
      }

      val coordTopic = Topic(s"persistent://public/darwindb/coord-${id}")

      val consumerFn = () => client.consumer(ConsumerConfig(subscriptionName = Subscription(s"$name-sub"),
        topics = Seq(coordTopic),
        subscriptionType = Some(SubscriptionType.Exclusive),
        subscriptionInitialPosition = Some(SubscriptionInitialPosition.Latest)),
      )

      val coordSource = source(consumerFn, Some(MessageId.latest))
      val lastSnk = Sink.last[Boolean]
      val sharedKillSwitch = KillSwitches.shared("coordinator-kill-switch")

      coordSource
        //.delay(1.second, DelayOverflowStrategy.backpressure)
        .via(sharedKillSwitch.flow)
        .mapAsync(1)(handler)
        .runWith(lastSnk)

      def close(): Future[Boolean] = {

        sharedKillSwitch.shutdown()
        batchTask.cancel()
        client.close()

        for {
          _ <- session.closeAsync().asScala
          //_ <- client.closeAsync().asScala
          // _ <- client.closeAsync().asScala
        } yield {
          true
        }
      }

      logger.info(s"\n${Console.MAGENTA_B}STARTING COORDINATOR ${name}...${Console.RESET}\n")

      Behaviors.receiveMessage[Command] {

        case Request(req, sender) =>

          val pr = Promise[TaskResponse]()
          queue.put(req.id, req -> pr)

          pr.future.map{sender ! _}

          Behaviors.same

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
  }.onFailure[Exception](SupervisorStrategy.restart)


}
