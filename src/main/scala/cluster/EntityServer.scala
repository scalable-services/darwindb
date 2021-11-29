package cluster

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.concurrent.Future
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import cluster.protocol.EntityServiceHandler
import scala.concurrent.duration._

import scala.util.{Failure, Success}

class EntityServer(port: Int, val system: ActorSystem[_], sharding: ClusterSharding) {

  implicit val sys = system
  import system.executionContext
  val logger = system.log

  def run(): Future[Http.ServerBinding] = {
    val service: HttpRequest => Future[HttpResponse] =
      EntityServiceHandler(new EntityServiceImpl(port, system, sharding))

    val bound: Future[Http.ServerBinding] = Http(system)
      .newServerAt(interface = "127.0.0.1", port = port)
      .bind(service)
      .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 10.seconds))

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        logger.info(s"${Console.MAGENTA_B}gRPC server bound to {}:{}${Console.RESET}", address.getHostString, address.getPort)
      case Failure(ex) =>
        logger.info(s"${Console.RED_B}Failed to bind gRPC endpoint, terminating system${Console.RESET}", ex)
        system.terminate()
    }

    bound
  }

}
