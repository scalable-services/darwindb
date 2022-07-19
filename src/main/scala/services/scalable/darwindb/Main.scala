package services.scalable.darwindb

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, ShardedDaemonProcess}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import com.typesafe.config.ConfigFactory

object Main {

  object RootBehavior {
    def apply(port: Int): Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
      val sharding = ClusterSharding(ctx.system)

      //sharding.init(Entity(TypeKey)(createBehavior = entityContext => Account(entityContext.entityId)))

      val daemon =  ShardedDaemonProcess(ctx.system)

      val singleton = ClusterSingleton(ctx.system)

      val proxy: ActorRef[Aggregator.Command] = singleton.init(
        SingletonActor(Behaviors.supervise(Aggregator("aggregator-0")).onFailure[Exception](SupervisorStrategy.restart),
          "Aggregator"))

      daemon.init("workers", WORKERS.length, id => Worker(WORKERS(id), id), Worker.Stop)
      daemon.init("coordinators", COORDINATORS.length, id => Coordinator(COORDINATORS(id), id), Coordinator.Stop)

      new CoordinatorServer("127.0.0.1", port + 1000, ctx.system, sharding).run()

      Behaviors.empty[Nothing]
    }
  }

  def startup(port: Int): Unit = {
    // Override the configuration of the port
    val config = ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port=$port
      akka.http.server.preview.enable-http2 = on
      """).withFallback(ConfigFactory.load())

    // Create an Akka system
    val system = ActorSystem[Nothing](RootBehavior(port), "DarwinDB", config)
  }

  def main(args: Array[String]): Unit = {

    val ports = Seq(2751, 2752, 2753)
    ports.foreach{port => startup(port)}

  }

}
