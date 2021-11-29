package cluster

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import cluster.protocol.HelloReply

object Account {

  val TypeKey = EntityTypeKey[Account.Command]("Account")

  trait Command extends CborSerializable
  case class HelloRequest(request: cluster.protocol.HelloRequest, sender: ActorRef[HelloReply]) extends Command

  def apply(id: String): Behavior[Command] = Behaviors.setup[Command] { ctx =>

    val logger = ctx.log

    logger.info(s"${Console.GREEN_B}Initializing entity ${id}...${Console.RESET}")

    Behaviors.receiveMessage {
      case HelloRequest(request, sender) =>
        sender ! HelloReply(s"Hello, ${request.name} from entity ${id}!")
        Behaviors.same

      case _ => Behaviors.same
    }
  }

}
