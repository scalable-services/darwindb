package cluster

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.GrpcClientSettings
import cluster.protocol.{EntityServiceClient, HelloReply, HelloRequest}
import com.typesafe.config.ConfigFactory
import io.grpc.netty.shaded.io.netty.util.internal.ThreadLocalRandom
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class ClientSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  "" should "" in {

    val rand = ThreadLocalRandom.current()
    val ports = Seq(3551, 3552, 3553)

    implicit val system = ActorSystem[Nothing](Behaviors.empty[Nothing], "Client", ConfigFactory.load("client.conf"))

    val settings = GrpcClientSettings.connectToServiceAt("127.0.0.1", ports(rand.nextInt(0, ports.length)))
      .withTls(false)
    val client = EntityServiceClient(settings)

    def send(name: String): Future[String] = {
      client.submit(HelloRequest(name)).map { response =>
        logger.info(s"${Console.GREEN_B}response: ${response}${Console.RESET}")
        response.message
      }
    }

    val requests = Seq("Luana", "Lucas", "Ivone", "Vilso").map{send(_)}

    Future.sequence(requests).onComplete {
      case _ => system.terminate()
    }

    Await.ready(system.whenTerminated, Duration.Inf)
  }

}
