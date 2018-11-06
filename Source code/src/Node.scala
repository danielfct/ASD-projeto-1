package com.example

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class Node(nId: Int, nRef: ActorRef) {
  var id = nId
  var ref = nRef
}

object ChordNode {
  def props(id: Int, m: Int): Props = Props(new ChordNode(id, m))
  
   final case object CreateFirstNode
   final case class CreateAndJoin(contactNode: ActorRef)
   final case class FindSuccessor(nodeId: Int, originalSender: ActorRef, requestType: String, index: Int)
   final case class FindSuccessorResponse(node: Node, originalSender: ActorRef, requestType: String, index: Int)
   final case class FindSuccessorComplete(node: Node, requestType: String, index: Int)
   final case class FindPredecessor(nodeId: Int, originalSender: ActorRef, requestType: String, index: Int)
   final case class FindPredecessorResponse(node: Node, originalSender: ActorRef, requestType: String, index: Int)
   final case class ClosestPrecedingFinger(nodeId: Int, originalSender: ActorRef, requestType: String, index: Int)
   final case class GetPredecessor(requestType: String, setNode: Node)
   final case class GetPredecessorResponse(node: Node, requestType: String)
   final case class GetSuccessor(originalSender: ActorRef, requestType: String, index: Int)
   final case class GetSuccessorResponse(node: Node, requestType: String, index: Int)
   final case class UpdatePredecessor(node: Node)
   final case object UpdatePredecessorResponse
   final case class UpdateFingerTable(node: Node, index: Int)
   final case class ClosestPrecedingFingerResponse(originalSender: ActorRef, node: Node, requestType: String, index: Int)
   final case class InitTableMayComplete(node: Node, requestType: String)
}

class ChordNode(myId: Int, m: Int) extends Actor with ActorLogging {
  import ChordNode._
  import ChordTester._
  
  override def preStart(): Unit = log.info("ChordNode{} started", myId)

  override def postStop(): Unit = log.info("ChordNode{} stopped", myId)
  
  var successor = new Node(0,null);
  var predecessor = new Node(0,null)
  var fingerTableStart = new Array[Int](m)
  var fingerTableNode = new Array[Node](m)
  var tester: ActorRef = null
  var contact: ActorRef = null;
  
  def closest_preceding_finger(id: Int): Node = {
    for (i<-(fingerTableNode.length-1) to 0) {
      if (fingerTableNode(i).id >= myId && fingerTableNode(i).id <= id)
        return fingerTableNode(i)
    }
    return new Node(myId, self)
  }
  
  override def receive: Receive = {
    case CreateFirstNode =>
      log.info("ChordNode{} got CreateFirstNode", myId)
      tester = sender
      val myself = new Node(myId, self);
      for (i<-0 to (m-1)) {
        fingerTableStart(i) = (myId + Math.pow(2,i).toInt) % Math.pow(2,m).toInt
        fingerTableNode(i) = myself
      }
      successor = myself
      predecessor = myself
      sender() ! JoinComplete
      
    case CreateAndJoin(contactNode) =>
      tester = sender
      contact = contactNode
      log.info("ChordNode{} got CreateAndJoin", myId)
      for(i<-0 to m-1)
        fingerTableStart(i) = (myId + Math.pow(2,i).toInt) % Math.pow(2,m).toInt
      contactNode ! FindSuccessor(fingerTableStart(0), self, "createAndJoin", -1) // init_finger_table(contactNode)
      
    case InitTableMayComplete(node, requestType) =>
      predecessor = node
      for (i<- 0 to (m-2)) {
          if (fingerTableStart(i+1) >= myId && fingerTableStart(i+1) <= fingerTableNode(i).id)
            fingerTableNode(i+1) = fingerTableNode(i)
          else contact ! FindSuccessor(fingerTableStart(i+1), self, "initTable", i+1)
      }
      
    case FindSuccessor(id, originalSender, requestType, index) =>
      self ! FindPredecessor(id, originalSender, requestType, index)
      
    case FindSuccessorResponse(node, originalSender, requestType, index) =>
      node.ref ! GetSuccessor(originalSender, requestType, index)      
      
    case FindSuccessorComplete(node, requestType, index) => 
      if (requestType == "createAndJoin") {
        fingerTableNode(0) = node
        successor.ref ! GetPredecessor(requestType, new Node(myId, self))
      } else if (requestType == "initTable") {
        fingerTableNode(index) = node
      }
      
    case FindPredecessor(id, originalSender, requestType, index) =>
     if (id < myId || id > successor.id) {
        val node = closest_preceding_finger(id);
        if (node.id < myId || node.id > successor.id)
          node.ref ! ClosestPrecedingFinger(id, originalSender, requestType, index)
        else originalSender ! FindPredecessorResponse(node, originalSender, requestType, index)
      } else originalSender ! FindPredecessorResponse(new Node(myId, self), originalSender, requestType, index)
      
    case FindPredecessorResponse(node, originalSender, requestType, index) =>
      if (requestType == "createAndJoin" || requestType == "initTable") {
        originalSender ! FindSuccessorResponse(node, originalSender, requestType, -1)
      } else { // updateOthers called
        
      }
      
    case ClosestPrecedingFinger(id, originalSender, requestType, index) =>
     val node = closest_preceding_finger(id);
      if (node.id < myId || node.id > successor.id)
        node.ref ! ClosestPrecedingFinger(id, originalSender, requestType, index)
      else originalSender ! ClosestPrecedingFingerResponse(originalSender, node, requestType, index)
      
    case ClosestPrecedingFingerResponse(originalSender, node, requestType, index) =>
      originalSender ! FindPredecessorResponse(node, originalSender, requestType, index)
      
    case GetPredecessor(requestType, setNode) =>
      val old_predecessor = predecessor
      if (setNode != null)
        predecessor = setNode
      sender() ! GetPredecessorResponse(old_predecessor, requestType)
      
    case GetPredecessorResponse(node, requestType) =>
      if (requestType == "createAndJoin") {
        self ! InitTableMayComplete(node, requestType)
      }
    
    case GetSuccessor(originalSender, requestType, index) =>
      originalSender ! GetSuccessorResponse(successor, requestType, index)
    
    case GetSuccessorResponse(node, requestType, index) =>
      self ! FindSuccessorComplete(node, requestType, index)
      
    case UpdatePredecessor(node) =>
      
    case UpdatePredecessorResponse =>
      
    case UpdateFingerTable(node, index) =>
      
  }
  
}

object ChordTester {
  def props(numNodes: Int, numRequests: Int, m: Int): Props = Props(new ChordTester(numNodes, numRequests, m))
  
  final case object Start
  final case object StartAndJoin
  final case object JoinComplete
}

class ChordTester(numNodes: Int, numRequests: Int, m: Int) extends Actor with ActorLogging {  
  import ChordTester._
  import ChordNode._
  
  var chordNodes = Map.empty[String, ActorRef]
  var nodesAlreadyCreated = 0
  var contactNode: ActorRef = null
  
  override def receive: Receive = {
    case Start =>
      val id = Random.nextInt()
      val actorName = "chordNode"+id;
      var originalSender = context.actorOf(ChordNode.props(id, m), actorName)
      contactNode = originalSender
      chordNodes += actorName -> originalSender
      originalSender ! CreateFirstNode
    case StartAndJoin =>
      val id = Random.nextInt();
      val actorName = "chordNode"+id;
      var originalSender = context.actorOf(ChordNode.props(id, m), actorName)
      chordNodes += actorName -> originalSender
      originalSender ! CreateAndJoin(contactNode)
    case JoinComplete =>
      nodesAlreadyCreated += 1;
      if (nodesAlreadyCreated < numNodes) {
        self ! StartAndJoin
      }
  }
}

object ChordDHT {
  import ChordTester._
  
  def main (args: Array[String]){
		var numNodes = 0 
		var numRequests = 0
		var m = 0

	    if (args.length != 2) {
	    	numNodes = 20;
	    	numRequests = 10;
	    } 
	    else {
	    	numNodes = args(0).toInt
	    	m = Math.max(math.ceil(math.log(numNodes)/math.log(2)).toInt,3)

	    	numRequests = args(1).toInt
	    	val system = ActorSystem("ChordSystem")
	    	val chordTester = system.actorOf(ChordTester.props(numNodes, numRequests, m), "ChordTester")
	    	chordTester ! Start
	    }
	}
}