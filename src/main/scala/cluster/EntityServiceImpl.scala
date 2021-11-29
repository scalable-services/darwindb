package cluster

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.util.Timeout
import cluster.protocol.{EntityService, HelloReply, HelloRequest}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class EntityServiceImpl(val port: Int, system: ActorSystem[_], sharding: ClusterSharding) extends EntityService {

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val timeout = Timeout(5 seconds)

  override def submit(in: HelloRequest): Future[HelloReply] = {
    val ref = sharding.entityRefFor(Account.TypeKey, in.name)

    ref.ask[HelloReply] { sender =>
      Account.HelloRequest(in, sender)
    }

    //Future.successful(HelloReply(s"Hi, ${in.name}!"))
  }

}
