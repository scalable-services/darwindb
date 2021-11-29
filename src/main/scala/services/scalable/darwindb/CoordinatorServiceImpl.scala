package services.scalable.darwindb

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.util.Timeout
import services.scalable.darwindb.protocol.{CoordinatorService, TaskRequest, TaskResponse}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class CoordinatorServiceImpl(val port: Int, system: ActorSystem[_], sharding: ClusterSharding) extends CoordinatorService {

  val logger = system.log

  implicit val timeout = Timeout(5 seconds)
  implicit val ec = system.executionContext
  implicit val scheduler = system.scheduler

  def findServiceRef(id: String): Future[Option[ActorRef[Coordinator.Command]]] = {
    val serviceKey = ServiceKey[Coordinator.Command](s"coordinator-${computeCoordinator(id)}")
    system.receptionist.ask[Receptionist.Listing] { actor =>
      Receptionist.Find(serviceKey, actor)
    }.map { listings =>
      listings.serviceInstances(serviceKey).headOption
    }
  }

  override def submit(request: TaskRequest): Future[TaskResponse] = {
    findServiceRef(request.id).flatMap {
      case None => Future.successful(TaskResponse(request.id, false))
      case Some(ref) => ref.ask[TaskResponse]{ sender =>
        Coordinator.Request(request, sender)
      }
    }
  }

}
