package com.example

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorLogging
import akka.pattern.ask
import scala.util.Random
import scala.concurrent.Future
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await

object Node {
  def props(contactNode: ActorRef): Props = Props(new Node(contactNode));
  
  final case class FindSuccessorRequest(id: Long)
  final case class FindSuccessorResponse(id: Long, ref: ActorRef)
}

class Pointer(var startId: Long, var nodeId: Long, var nodeRef: ActorRef);

class Node(contactNode: ActorRef) extends Actor with ActorLogging {
  import Node._
  val id = Random.nextLong();
  var successorId: Option[Long] = None;
  var successorRef: Option[ActorRef] = None;
  var predecessorId: Option[Long] = None;
  var predecessorRef: Option[ActorRef] = None;
  var fingers: Array[Pointer] = new Array[Pointer](3);
  
  private def find_successor(id: Long) = {
    
  }
  
  private def find_predecessor(rid: Long) = {
    if (rid < id && successorId.exists(_ < id)) {
       val ref: ActorRef = closest_preceding_finger(rid);
       implicit val timeout = Timeout(5 seconds)
      val future: Future[FindSuccessorResponse] = ask(ref, FindSuccessorRequest(rid)).mapTo[FindSuccessorResponse]
      val result = Await.result(future, 1 second)
      
    }
  }
  
  private def closest_preceding_finger(rid: Long): ActorRef = {
    for (i <- fingers.length - 1 to 1) {
      if (fingers(i).nodeId >= id && fingers(i).nodeId <= rid) {
        return fingers(i).nodeRef;
      }
    }
    return self;
  }
  
  override def receive = Actor.emptyBehavior
}