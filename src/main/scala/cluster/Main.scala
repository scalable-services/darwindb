package cluster

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import cluster.Account.TypeKey
import com.typesafe.config.ConfigFactory

object Main {

  object RootBehavior {
    def apply(port: Int): Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
      val sharding = ClusterSharding(ctx.system)

      sharding.init(Entity(TypeKey)(createBehavior = entityContext => Account(entityContext.entityId)))

      new EntityServer(port + 1000, ctx.system, sharding).run()

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
    val system = ActorSystem[Nothing](RootBehavior(port), "EntitySystem", config)
  }

  def main(args: Array[String]): Unit = {

    val ports = Seq(2551, 2552, 2553)
    ports.foreach{port => startup(port)}

  }

}
