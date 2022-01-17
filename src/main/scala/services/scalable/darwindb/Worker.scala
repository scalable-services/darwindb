package services.scalable.darwindb

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{Behavior, PostStop, SupervisorStrategy}
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.sksamuel.pulsar4s.akka.streams.{sink, source}
import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{Schema, SubscriptionInitialPosition, SubscriptionType}
import services.scalable.darwindb.protocol.{Batch, BatchDone, Position, TaskRequest, VoteBatch}
import services.scalable.datalog.{CQLStorage, DatomDatabase}
import services.scalable.datalog.grpc.Datom

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.FutureConverters._
import scala.util.{Failure, Success}
import services.scalable.datalog.DefaultDatalogSerializers.grpcBlockSerializer
import services.scalable.index.{AsyncIterator, Bytes, Tuple}
import services.scalable.index.DefaultComparators.ord
import services.scalable.index.impl.DefaultCache

/**
 * TODO Implement the logic for saving the state of the worker before failure.
 */
object Worker {

  trait Command extends CborSerializable

  final case object Stop extends Command

  def apply(name: String, id: Int): Behavior[Command] = Behaviors.supervise {
    Behaviors.setup[Command] { ctx =>

      val logger = ctx.log
      implicit val ec = ctx.executionContext

      val session = CqlSession
        .builder()
        //.addContactPoint(new InetSocketAddress(Config.CASSANDRA_HOST, Config.CASSANDRA_PORT))
        .withConfigLoader(Config.loader)
        //.withLocalDatacenter(Config.DC)
        .withKeyspace(Config.KEYSPACE)
        .build()

      val dbSession = CqlSession
        .builder()
        .withConfigLoader(Config.loader)
        .withKeyspace(Config.DB_KEYSPACE)
        .build()

      implicit val classicSystem: ActorSystem = ctx.system.classicSystem
      implicit val schema: Schema[Array[Byte]] = Schema.BYTES

      val config = PulsarClientConfig(serviceUrl = Config.PULSAR_SERVICE_URL, allowTlsInsecureConnection = Some(true))
      val client = PulsarClient(Config.PULSAR_SERVICE_URL)

      val topologyTopic = Topic(Config.Topics.TOPOLOGY_LOG)

      var coordinators = Map.empty[Int, () => Producer[Array[Byte]]]
      var workers = Map.empty[Int, () => Producer[Array[Byte]]]

      for(i<-0 until Config.NUM_COORDINATORS){
        val producer = () => client.producer[Array[Byte]](ProducerConfig(topic = Topic(s"persistent://public/darwindb/coord-${i}"),
          enableBatching = Some(false), blockIfQueueFull = Some(false)))
        coordinators = coordinators + Tuple2(i, producer)
      }

      for(i<-0 until Config.NUM_WORKERS){
        val producer = () => client.producer[Array[Byte]](ProducerConfig(topic = Topic(s"persistent://public/darwindb/worker-${i}"),
          enableBatching = Some(false), blockIfQueueFull = Some(false)))
        workers = workers + Tuple2(i, producer)
      }

      def notifyCoordinator(b: BatchDone): Unit = {
        val producer = coordinators(b.coordinator)
        val record = ProducerMessage[Array[Byte]](Any.pack(b).toByteArray)

        Source.single(record).to(sink(producer)).run()
      }

      def notifyWorker(b: BatchDone): Unit = {
        val buf = Any.pack(b).toByteArray
        val record = ProducerMessage[Array[Byte]](buf)

        b.workers.foreach { w =>
          val producer = workers(w)
          Source.single(record).to(sink(producer)).run()
        }
      }

      val consumerFn = () => client.consumer(ConsumerConfig(subscriptionName = Subscription(s"$name-topo"),
        topics = Seq(topologyTopic),
        subscriptionType = Some(SubscriptionType.Exclusive),
        subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest)),
      )

      val lastTopologySnk = Sink.last[Boolean]
      val sharedTopologyKillSwitch = KillSwitches.shared("worker-topology-kill-switch")
      val topologySource = source(consumerFn, Some(MessageId.latest))

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

      var promise = Promise[Boolean]()

      var b: Batch = null
      val queue = new ConcurrentLinkedQueue[Batch]()

      def vote(batches: Seq[Batch]): Unit = {
        batches.groupBy(_.leader).foreach { case (leader, list) =>
          val producer = workers(leader)

          val v = VoteBatch(id, list.map(_.id))

          val record = ProducerMessage[Array[Byte]](Any.pack(v).toByteArray)
          Source.single(record).to(sink(producer)).run()
        }
      }

      def next(): Unit = {

        b = queue.poll()

        if(b == null){
          promise.success(true)
          return
        }

        // logger.info(s"${Console.MAGENTA_B}$name processing ${b.id} with workers ${b.workers.sorted}${Console.RESET}")

        vote(Seq(b))
      }

      def retrieve(cur: MessageId, end: MessageId, r1: Reader[Array[Byte]]): Future[Seq[Batch]] = {
        if(cur.compareTo(end) == 0) {
          return Future.successful(Seq.empty[Batch])
        }

        r1.nextAsync.flatMap { case m =>
          val b = Any.parseFrom(m.value).unpack(Batch)
          retrieve(m.messageId, end, r1).map{b +: _}
        }
      }

      def handler(msg: ConsumerMessage[Array[Byte]]): Future[Boolean] = {
        val c = Any.parseFrom(msg.value).unpack(Position)

        val cur = org.apache.pulsar.client.api.MessageId.fromByteArray(c.start.toByteArray)
        val end = org.apache.pulsar.client.api.MessageId.fromByteArray(c.end.toByteArray)

        val readerConfig = ReaderConfig(topic = Topic(s"persistent://public/darwindb/log-partition-${c.p}"), startMessage = Message(cur),
          startMessageIdInclusive = false, reader = Some(s"$name-reader-p${c.p}"))
        val r1 = client.reader(readerConfig)

        //val r1 = readers(c.p)

        promise = Promise[Boolean]()

        /* r1.seekAsync(cur).flatMap(_ => retrieve(cur, end, r1))*/retrieve(cur, end, r1).onComplete {
          case Success(records) =>

            /*var records = Seq.empty[Batch]

            while(cur.compareTo(end) < 0){
              val m = r1.next
              val r = Any.parseFrom(m.value).unpack(Batch)

              records = records :+ r

              cur = m.messageId
            }*/

            r1.closeAsync.onComplete {
              case Success(ok) =>

                logger.info(s"${Console.MAGENTA_B}$name topology for log-partition-${c.p} ${msg.messageId} => ${records.map(_.id)} ${Console.RESET}")

                val local = records.filter(_.workers.contains(id))

                if(local.isEmpty){
                  //return Future.successful(true)
                  promise.success(true)
                } else {
                  local.foreach{b => queue.add(b)}
                  next()
                }

              case Failure(ex) => ex.printStackTrace()
            }


          case Failure(ex) => ex.printStackTrace()
        }

        promise.future
      }

      topologySource
        //.via(sharedTopologyKillSwitch.flow)
        .mapAsync(1)(handler)
        .run()
        //.runWith(lastTopologySnk)

      val statusTopic = Topic(s"persistent://public/darwindb/worker-${id}")

      val sconsumerFn = () => client.consumer(ConsumerConfig(subscriptionName = Subscription(s"$name-status"), topics = Seq(statusTopic),
        subscriptionType = Some(SubscriptionType.Exclusive), subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest)))

      val lastStatusSnk = Sink.last[Boolean]
      val sharedStatusKillSwitch = KillSwitches.shared("worker-status-kill-switch")
      val statusSource = source(sconsumerFn, Some(MessageId.latest))

      val votes = TrieMap.empty[String, Seq[Int]]

      val NUM_LEAF_ENTRIES = 64
      val NUM_META_ENTRIES = 64

      val EMPTY_ARRAY = Array.empty[Byte]

      implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
      implicit val storage = new CQLStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES, dbSession)

      val termOrd = new Ordering[Datom] {
        override def compare(x: Datom, y: Datom): Int = {
          val r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

          if(r != 0) return r

          ord.compare(x.getA.getBytes(), y.getA.getBytes())
        }
      }

      def loadOne[K, V](it: AsyncIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Option[Tuple[K, V]]] = {
        it.hasNext().flatMap {
          case true => it.next().map { list =>
            list.headOption
          }
          case false => Future.successful(None)
        }
      }

      def find(a: String, id: String, db: DatomDatabase, reload: Boolean): Future[Option[Datom]] = {
        if(reload) {
          return db.load().flatMap { _ =>
            val it = db.eavtIndex.find(Datom(e = Some(id), a = Some(a)), false, termOrd)
            loadOne(it).map(_.headOption.map(_._1))
          }
        }

        val it = db.eavtIndex.find(Datom(e = Some(id), a = Some(a)), false, termOrd)
        loadOne(it).map(_.headOption.map(_._1))
      }

      def readData(id: String, db: DatomDatabase, reload: Boolean): Future[(String, Int)] = {
        find("users/:balance", id, db, reload).map(_.get).map { d =>
          d.getTx -> java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()
        }
      }

      def writeData(k: String, value: Int, tx: String, db: DatomDatabase): Future[Boolean] = {
        val balance = java.nio.ByteBuffer.allocate(4).putInt(value).flip().array()

        for {
          ok1 <- db.update(
            Seq(
              Datom(
                e = Some(k),
                a = Some("users/:balance"),
                v = Some(ByteString.copyFrom(balance)),
                tx = Some(tx),
                tmp = Some(System.currentTimeMillis())
              ) -> Array.empty[Byte]
            )
          )

          ok <- if(ok1) db.save() else Future.successful(false)
        } yield {
          ok
        }
      }

      def executeBatch(b: Batch): Future[Seq[(String, Boolean)]] = {

        def loadDB(k: String): Future[DatomDatabase] = {
          val db = new DatomDatabase(k, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, dbSession, grpcBlockSerializer, cache, storage)
          db.load().map(_ => db)
        }

        def execute(t: TaskRequest): Future[Boolean] = {
          val keys = (t.reads.keys ++ t.writes.keys).toSeq.distinct

          Future.sequence(keys.map{k => loadDB(k).map{k -> _}}).flatMap { dbs =>
            val dbMap = dbs.toMap

            Future.sequence(t.reads.map{case (k, v) => readData(k, dbMap(k), true).map { case (lv, _) =>
              k -> (lv.compareTo(v) == 0 || lv.compareTo(t.id) == 0)
            }}).flatMap { changed =>
              if(changed.forall(_._2 == true)){
                Future.sequence(t.writes.map{ case (k, value) => writeData(k, value, t.id, dbMap(k))}).map(_ => true)
              } else {
                Future.successful(false)
              }
            }
          }
        }

        Future.sequence(b.tasks.map{t => execute(t).map{t.id -> _}})
      }

      def shandler(msg: ConsumerMessage[Array[Byte]]): Future[Boolean] = {
        val parsed = Any.parseFrom(msg.value)

        if(parsed.is(VoteBatch)){

          val v = parsed.unpack(VoteBatch)

          v.metas.foreach { t =>
            votes.get(t) match {
              case None => votes.put(t, Seq(v.from))
              case Some(list) => votes.put(t, list :+ v.from)
            }
          }

          logger.info(s"${Console.YELLOW_B}$name votes ${votes} batch ${if(b != null) b.id else ""}${Console.RESET}")

          /*if(b != null && votes.isDefinedAt(b.id) && b.workers.forall(votes(b.id).contains(_))){
            votes.remove(b.id)

            logger.info(s"${Console.GREEN_B}$name executing ${b.id} with workers ${b.workers.sorted}${Console.RESET}")

            val done = BatchDone(b.id, b.coordinator, b.tasks.map(_.id)).withWorkers(b.workers)

            notifyCoordinator(done)
            notifyWorker(done)
          }

          return Future.successful(true)*/

          if(b != null && votes.isDefinedAt(b.id) && b.workers.forall(votes(b.id).contains(_))){
            votes.remove(b.id)

            logger.info(s"${Console.GREEN_B}$name executing ${b.id} with workers ${b.workers.sorted}${Console.RESET}")

            return executeBatch(b).map { results =>

              val successes = results.filter(_._2).map(_._1)
              val failures = results.filter(!_._2).map(_._1)

              val done = BatchDone(b.id, b.coordinator, successes, failures).withWorkers(b.workers)

              notifyCoordinator(done)
              notifyWorker(done)

              true
            }
          }

          return Future.successful(true)
        }

        val done = parsed.unpack(BatchDone)

        logger.info(s"${Console.RED_B}$name received done ${done.id} with workers ${b.workers.sorted}${Console.RESET}")
        assert(done.id.compareTo(b.id) == 0)

        next()

        //promise.success(true)

        Future.successful(true)
      }

      statusSource
        //.via(sharedStatusKillSwitch.flow)
        .mapAsync(1)(shandler)
        .run()
        //.runWith(lastStatusSnk)

      def close(): Future[Boolean] = {

        sharedStatusKillSwitch.shutdown()
        sharedTopologyKillSwitch.shutdown()
        client.close()

        for {
          _ <- dbSession.closeAsync().asScala
          _ <- session.closeAsync().asScala
          // _ <- consumer.closeAsync().toScala
          // _ <- client.closeAsync().toScala
        } yield {
          true
        }
      }

      logger.info(s"\n${Console.GREEN_B}STARTING WORKER ${name}...${Console.RESET}\n")

      Behaviors.receiveMessage[Command] {
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
  }.onFailure[Exception](SupervisorStrategy.restart)

}
