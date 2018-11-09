package com.example

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks._

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
   final case class GetAndUpdatePredecessor(requestType: String, setNode: Node)
   final case class GetAndUpdatePredecessorResponse(node: Node, requestType: String)
   final case class GetSuccessor(originalSender: ActorRef, requestType: String, index: Int)
   final case class GetSuccessorResponse(node: Node, requestType: String, index: Int)
   final case class UpdatePredecessor(node: Node)
   final case object UpdatePredecessorResponse
   final case class UpdateFingerTable(node: Node, index: Int)
   final case class ClosestPrecedingFingerResponse(originalSender: ActorRef, node: Node, requestType: String, index: Int)
   final case class InitTableMightComplete(node: Node, requestType: String)
   final case object UpdateOthers
   final case class UpdateOthersMayComplete(node: Node, index: Int)
   
   //For testing:
   final case object GetStatus
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
  
  var initTableInProgress = false
  var initTableFirstTime = true
  var initTableStartIndex = 0
  
  def closest_preceding_finger(id: Int): Node = {
    log.info("ChordNode{} called closest_preceding_finger({})", myId, id)
    for (i<-(m-1) to 0 by -1) {
      //log.info("fingerTableNode({}).id = {}, myId={}, id={}", i, fingerTableNode(i).id, myId, id)
      if (isInRange(fingerTableNode(i).id,myId,id,false,false)) //  fingerTableNode(i).id >= myId && fingerTableNode(i).id <= id
        return fingerTableNode(i)
    }
    return new Node(myId, self)
  }
  
  def isInRange(value: Int, start: Int, end: Int, includeStart: Boolean, includeEnd: Boolean): Boolean = {
    log.info("Chord{} -> isInRange value={}, start={}, end={}", myId, value, start, end);
    log.info("includeStart={}, includeEnd={}", includeStart, includeEnd)
    Thread.sleep(1000)
    var res = false
    
    // 4 >= 2 && 4 < 3               4 € [2, 3[ == [2, 3] false
    // 4 >= 3 || 4 < 1               4 € [3, 1[ == [3, 0] true
    // 3 >= 3 || 3 < 3               3 € ]3, 3] == [2, 3] true

    start = if (includeStart) start else (if (start < end) start + 1 else start - 1); //TODO % Math.pow(2,m).toInt
    end = if (includeEnd) end else (if (end > start) end - 1 else end +1 1); //TODO % Math.pow(2,m).toInt

    if (start < end) {
        res = value >= start && value < end;
    } else if (end >= start) {
        res = value >= start || value < end; 
    } 

    log.info("res={}", res)
    return res;
  }

/*
    if (start == end && value != start && includeStart != includeEnd) {
      log.info("res={}", successor.id == myId)
      return successor.id == myId
    }
    
    if (includeStart) {
      if (includeEnd) { // [start,end]
        if (start <= end) res = value >= start && value <= end
        else res = value >= start || value <= end
      } else { // [start,end[
        if (start < end) res = value >= start && value < end
        else res = value >= start || value < end
      }
    } else if (includeEnd) { // ]start,end]
        if (start < end) res = value > start && value <= end
        else res = value > start || value <= end
    } else { // ]start,end[
        if (start < end) res = value > start && value < end
        else res = value > start || value < end
    }
    log.info("res={}", res)
    return res;
  }*/
  
  def getHigherId(startId: Int, oldId: Int, newId: Int) : Int = {
   if (startId != oldId && startId != newId) {
     if(isInRange(startId,oldId,newId,false,false)) return newId else return oldId
   } else if (startId != oldId && startId == newId) {
     if(isInRange(startId,oldId,newId,false,true)) return newId else return oldId
   } else if(startId == oldId && startId != newId) {
     if(isInRange(startId,oldId,newId,false,true)) return newId else return oldId
   } else return oldId
  }
  
  override def receive: Receive = {
    case CreateFirstNode =>
      log.info("ChordNode{} got CreateFirstNode", myId)
      Thread.sleep(1000)
      tester = sender
      val myself = new Node(myId, self);
      for (i<-0 to (m-1))
        fingerTableNode(i) = myself
      successor = myself
      predecessor = myself
      sender() ! JoinComplete
      
    case CreateAndJoin(contactNode) =>
      tester = sender
      contact = contactNode
      log.info("ChordNode{} got CreateAndJoin", myId)
      Thread.sleep(1000)
      for(i<-0 to m-1)
        fingerTableStart(i) = (myId + Math.pow(2,i).toInt) % Math.pow(2,m).toInt
      contactNode ! FindSuccessor(fingerTableStart(0), self, "createAndJoin", -1) // init_finger_table(contactNode)
      
    case InitTableMightComplete(node, requestType) =>
      log.info("ChordNode{} got InitTableMightComplete with node={}, requestType={}", myId, node.id, requestType)
      Thread.sleep(1000)
      if (initTableFirstTime) {
        predecessor = node
        initTableFirstTime = false
      }
      var completed = true
      log.info("initTableStartIndex={}, m={}", initTableStartIndex, m);
      breakable { for (i<-initTableStartIndex to (m-2)) {
        log.info("iteration i={}, will check {}",i,i+1)
          if (isInRange(fingerTableStart(i+1),myId,fingerTableNode(i).id,true,false)) // fingerTableStart(i+1) >= myId && fingerTableStart(i+1) <= fingerTableNode(i).id
            fingerTableNode(i+1) = fingerTableNode(i)
          else {
            completed = false
            if (i+1 <= (m-2)) {
              initTableInProgress = true
              initTableStartIndex = i+1
            } else initTableInProgress = false
            contact ! FindSuccessor(fingerTableStart(i+1), self, "initTable", i+1)
            break
          }
      } }
      if (completed) self ! UpdateOthers
      
    case FindSuccessor(id, originalSender, requestType, index) =>
      log.info("ChordNode{} got FindSuccessor with id={}, originalSender={}, requestType={}", myId,id,originalSender,requestType)
      Thread.sleep(1000)
      self ! FindPredecessor(id, originalSender, requestType, index)
      
    case FindSuccessorResponse(node, originalSender, requestType, index) =>
      log.info("ChordNode{} got FindSuccessorResponse with id={}, originalSender={}, requestType={}", myId,node.id,originalSender,requestType)
      Thread.sleep(1000)
      node.ref ! GetSuccessor(originalSender, requestType, index)      
      
    case FindSuccessorComplete(node, requestType, index) => 
      log.info("ChordNode{} got FindSuccessorComplete with id={}, requestType={}, index={}", myId,node.id,requestType,index)
      Thread.sleep(1000)
      if (requestType == "createAndJoin") {
        fingerTableNode(0) = node
        successor = node
        successor.ref ! GetAndUpdatePredecessor(requestType, new Node(myId, self))
      } else if (requestType == "initTable") {
        fingerTableNode(index) = node // initTable ended
        log.info("fingerTableNode({}) was set to {}", index, node)
        if (!initTableInProgress)
          self ! UpdateOthers // update_others()
        else self ! InitTableMightComplete(new Node(-1,null),requestType)
      }
      
    case FindPredecessor(id, originalSender, requestType, index) =>
     log.info("ChordNode{} got FindPredecessor with id={}, originalSender={}, requestType={}", myId,id,originalSender,requestType)
     Thread.sleep(1000)
     if (!isInRange(id,myId,successor.id,false,true)) { //  id < myId || id > successor.id
        val node = closest_preceding_finger(id);
        if (!isInRange(node.id,myId,successor.id,false,true)) //   node.id < myId || node.id > successor.id
          node.ref ! ClosestPrecedingFinger(id, originalSender, requestType, index)
        else originalSender ! FindPredecessorResponse(node, originalSender, requestType, index)
      } else originalSender ! FindPredecessorResponse(new Node(myId, self), originalSender, requestType, index)
      
    case FindPredecessorResponse(node, originalSender, requestType, index) =>
      log.info("ChordNode{} got FindPredecessorResponse with id={}, originalSender={}, requestType={}", myId,node.id,originalSender,requestType)
      Thread.sleep(1000)
      if (requestType == "createAndJoin" || requestType == "initTable") {
        originalSender ! FindSuccessorResponse(node, originalSender, requestType, index)
      } else { // updateOthers called
        originalSender ! UpdateOthersMayComplete(node, index)
      }
      
    case ClosestPrecedingFinger(id, originalSender, requestType, index) =>
      log.info("ChordNode{} got ClosestPrecedingFinger with id={}, originalSender={}, requestType={}", myId,id,originalSender,requestType)
      Thread.sleep(1000)
     val node = closest_preceding_finger(id);
      if (!isInRange(node.id,myId,successor.id,false,false)) //  node.id < myId || node.id > successor.id
        node.ref ! ClosestPrecedingFinger(id, originalSender, requestType, index)
      else originalSender ! ClosestPrecedingFingerResponse(originalSender, node, requestType, index)
      
    case ClosestPrecedingFingerResponse(originalSender, node, requestType, index) =>
      log.info("ChordNode{} got ClosestPrecedingFingerResponse with id={}, originalSender={}, requestType={}", myId,node.id,originalSender,requestType)
      Thread.sleep(1000)
      originalSender ! FindPredecessorResponse(node, originalSender, requestType, index)
      
    case GetAndUpdatePredecessor(requestType, setNode) =>
      log.info("ChordNode{} got GetAndUpdatePredecessor requestType={}, setNode={}", myId,requestType,setNode.id)
      Thread.sleep(1000)
      val old_predecessor = predecessor
      if (setNode != null)
        predecessor = setNode
      sender() ! GetAndUpdatePredecessorResponse(old_predecessor, requestType)
      
    case GetAndUpdatePredecessorResponse(node, requestType) =>
      log.info("ChordNode{} got GetAndUpdatePredecessorResponse requestType={}", myId,requestType)
      Thread.sleep(1000)
      if (requestType == "createAndJoin") {
        self ! InitTableMightComplete(node, requestType)
      }
    
    case GetSuccessor(originalSender, requestType, index) =>
      log.info("ChordNode{} got GetSuccessor with originalSender={}, requestType={}, index={}", myId,originalSender,requestType, index)
      Thread.sleep(1000)
      originalSender ! GetSuccessorResponse(successor, requestType, index)
    
    case GetSuccessorResponse(node, requestType, index) =>
      log.info("ChordNode{} got GetSuccessorResponse with id={}, requestType={}, index={}", myId,node.id,requestType, index)
      Thread.sleep(1000)
      self ! FindSuccessorComplete(node, requestType, index)     
      
    case UpdateOthers =>
      log.info("ChordNode{} got UpdateOthers")
      log.info("My successor is " + successor.id )
      log.info("My predecessor is " + predecessor.id)
      Thread.sleep(1000)
      for(i<-0 to (m-1))
        log.info("finger{}: start={} succ={}",i,fingerTableStart(i), fingerTableNode(i).id);
      for (i<-0 to (m-1))
        self ! FindPredecessor(myId - Math.pow(2,i).toInt,self,"updateOthers",i)
        
    case UpdateOthersMayComplete(node, index) =>
      log.info("ChordNode{} got UpdateOthersMayComplete with id={}, index={}", myId,node.id, index)
      Thread.sleep(1000)
      node.ref ! UpdateFingerTable(new Node(myId, self), index)
      if (index == (m-1))
        tester ! JoinComplete
      
    case UpdateFingerTable(s, i) =>
      log.info("ChordNode{} got UpdateFingerTable with id={}, index={}", myId,s.id, i)
      Thread.sleep(1000)
      
      if (isInRange(s.id,myId,fingerTableNode(i).id,true,false)) { //  s.id >= myId && s.id <= fingerTableNode(i).id
        if (sender != self) {
          fingerTableNode(i) = s
          if(i == 0) successor = s
        }
        if (predecessor.ref != sender && predecessor.ref != self)
          predecessor.ref ! UpdateFingerTable(s,i)
        
        self ! GetStatus
      } else if (myId == fingerTableNode(i).id) {
        val startId = myId + Math.pow(2,i).toInt
        var higher = getHigherId(startId,fingerTableNode(i).id,s.id)
          if (s.id == higher) {
            fingerTableNode(i) = s
            if(i == 0) successor = s
          }
        
        self ! GetStatus
      }
      
    case GetStatus =>
      log.info("Status for ChordNode{}:", myId);
      log.info("{}: My predecessor is {}", myId, predecessor.id)
      log.info("{}: My successor is {}", myId, successor.id)
      for (i<-0 to (m-1)) {
        if (fingerTableStart(i) > 0)
          log.info("{}: fingerTableStart({})={}", myId, i, fingerTableStart(i))
        log.info("{}: fingerTableNode({})={}",myId, i,fingerTableNode(i).id)
      }
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
  var s1: ActorRef = null
  var s2: ActorRef = null
  
  override def receive: Receive = {
    case Start =>
      var id = 0
      do {
        id = 3
      } while(chordNodes.contains("chordNode"+id))
        
      val actorName = "chordNode"+id;
      var originalSender = context.actorOf(ChordNode.props(id, m), actorName)
      contactNode = originalSender
      s1 = originalSender
      chordNodes += actorName -> originalSender
      originalSender ! CreateFirstNode
    case StartAndJoin =>
      var id = 0
      do {
        id = 2
      } while(chordNodes.contains("chordNode"+id))
        
      val actorName = "chordNode"+id;
      var originalSender = context.actorOf(ChordNode.props(id, m), actorName)
      s2 = originalSender
      chordNodes += actorName -> originalSender
      originalSender ! CreateAndJoin(contactNode)
    case JoinComplete =>
      nodesAlreadyCreated += 1;
      if (nodesAlreadyCreated == 2) {
        s1 ! GetStatus
      }
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
	    	numNodes = 2;
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