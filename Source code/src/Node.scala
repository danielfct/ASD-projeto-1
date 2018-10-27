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
import akka.actor.ActorSystem
import java.security.MessageDigest
import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit

//import system.dispatcher

final case class lookUp(n:Int)
final case class lookUpFinish()
final case class forwardLookUp(key:Int, node:Int)

object Chord_DHT
{
  def main(args: Array[String])
  {
    var numNodes: Int = 0;
    var numRequests: Int = 0;
    var numFails: Int = 0;
    
    if(args.length < 2 || args.length > 2)
    {
      numNodes = 256;
      numRequests = 3;  
      println(" *ERROR*: Illegal number of arguments.");
      println(" Taking default values of number of Nodes as "+numNodes+" and number of requests as "+numRequests);         
    }
    else
    {
      numNodes = args(0).toInt;
      numRequests = args(1).toInt;
     // numFails = args(3).toInt;  
    }
    
    //Convert input number of nodes to nearest power of two
/*    var temp:Double =0;
    temp = math.log(numNodes)/math.log(2)
    temp = temp.ceil
    numNodes = (math.pow(2, temp)).toInt;
 */
  
    //Create system for master node and initiate it
    val superSystem = ActorSystem("SuperNodeSys");
    var master = superSystem.actorOf(Props(new SuperNode(numNodes,numRequests)), name="SuperNode")
    
    
  }
}

//#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$
//    Super node definition
//#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$

class SuperNode(nodeCount:Int, reqCount:Int) extends Actor
{
    var numNodes = nodeCount
    var numRequest = reqCount
    var totalHops:Double = 0
    var totalLookUps:Double = 0
    
    val system = ActorSystem("workerNodes");
    var nodeList = new Array[ActorRef](numNodes)
    var rndm = new Random()
    
    //make n nodes and construct the network topology
    println("Constructing node network ring...")
    for( i <- 0 to numNodes-1)
    {
      nodeList(i) = system.actorOf(ChordNode.props(Random.nextLong(), numNodes, nodeList(Random.nextInt(i))), name = "node"+i)
    }
    
    
    //write a snippet that generates a random number and sends that as a request to a node
    
    println("Running 1 request per second on each actor...")
    
    
      for(j <- 0 to numRequest-1)
      {
         for(i <- 0 to numNodes-1)
         {
            var a = rndm.nextInt(numNodes)
            nodeList(i) ! lookUp(a)
            //println("requesting "+a+" to "+i)
          }
         Thread.sleep(1000)
         println(j+1+" Requests per actor processed")
      }
   
    
    /*
     * Receive method in super node has a case for lookup finish, when a node sends back this then
     * super node increments counter for hops and lookup requests completed. When lookup requests 
     * are equal to total requested in beginning then result is computed and printed.
    */
    def receive =
    {
      //case 1 of 3
      case lookUpFinish() =>
      {
          totalHops += 1
          totalLookUps += 1
          //println("finish reported")
          if(totalLookUps == numNodes*numRequest)
          {
            println("Desired number of requests have been completed ")
            println("Each node made "+numRequest+" requests")
            println("Total requests processed "+totalLookUps.toInt+", Total hops traversed "+totalHops.toInt)
            println("Average hop count for each lookup comes out to be "+(totalHops/totalLookUps)) 
            System.exit(1);
          }
      }
      //case 2 of 3
      case forwardLookUp(key,node) =>
      {
        //println("forward key "+key+" to node "+node)
        totalHops += 1
        nodeList(node) ! lookUp(key)    
      }
      //case 3 of 3
      case _ => 
        {
          println("Default case in Super Node")
        }
    }
}

//#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$
//   worker node starts
//#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$

object ChordNode {
  def props(id: Long, nodeCount: Int, contactNode: ActorRef): Props = Props(new ChordNode(id, nodeCount, contactNode))

  final case class FindSuccessorRequest(nodeId: Long);
  final case class FindSuccessorResponse(successorId: Long, successorRef: ActorRef);
  final case class FindPredecessorRequest(nodeId: Long);
  final case class FindPredecessorResponse(predecessorId: Long, predecessorRef: ActorRef);
  final case class UpdatePredecessorRequest(predecessorId: Long, predecessorRef: ActorRef);
  final case class UpdatePredecessorResponse(oldPredecessorId: Long, oldPredecessorRef: ActorRef);
  final case object GetSuccessorRequest;
  final case class GetSuccessorResponse(successorId: Long, successorRef: ActorRef);
  final case object GetPredecessorRequest;
  final case class GetPredecessorResponse(predecessorId: Long, predecessorRef: ActorRef);
  final case class NotifySuccessorRequest(nodeId: Long, nodeRef: ActorRef);
  final case class NotifySuccessorResponse(); // ???
  final case object HeartbeatRequest;
  final case class HeartbeatResponse();
}

class FingerEntry(var startId: Long, var nodeId: Long, var nodeRef: ActorRef);

class ChordNode(id: Long, nodeCount: Int, contactNode: ActorRef) extends Actor with ActorLogging
{
  import ChordNode._
  var numNodes: Int = nodeCount;
  var nodeId: Long = id;
  var successorId: Long = nodeId;  
  var successorRef: ActorRef = self;
  var predecessorId: Long = 0;
  var predecessorRef: ActorRef = null;
  
  var fix_fingers_next: Int = 0; // To be confirmed...
  
  var hopCount:Int = 0;
  
  //get hash of nodeId as an array of Byte
  var hashResult: String = sha1Hash(nodeId.toString);
  
  //construct a finger table for this node instance
  val m: Int = (math.log(numNodes)/math.log(2)).toInt;
  var fingerTable = new Array[FingerEntry](m);
  
  val system = ActorSystem("chordNodeSystem") // Not sure if we can do this...
  import system.dispatcher
  
  val interval = Duration(100, TimeUnit.SECONDS)
  system.scheduler.schedule(Duration(50, TimeUnit.SECONDS), interval, new Runnable{def run(){stabilize()}})
  system.scheduler.schedule(Duration(120, TimeUnit.SECONDS), interval, new Runnable{def run(){fix_fingers()}})
  system.scheduler.schedule(Duration(120, TimeUnit.SECONDS), interval, new Runnable{def run(){check_predecessor()}})
  
/* 
 	//ask node n to find id's successor
	Upon find_successor(id) do:
	(nodeId,nodeRef) <- trigger find_predecessor(id)
	(succId,succRef) <- SendAndWait(FIND_SUCCESSOR, nodeId, nodeRef)
	return (succId,succRef)*/
  
  private def find_successor(id: Long): (Long, ActorRef) = {
    val (_, nodeRef: ActorRef) = find_predecessor(id);
    implicit val timeout = Timeout(60 seconds);
    val future = nodeRef ? GetSuccessorRequest;
    val result = Await.result(future, timeout.duration).asInstanceOf[GetSuccessorResponse];
    return (result.successorId, result.successorRef);
  }
  
  /*
  //ask node n to find id's predecessor
	Upon find_predecessor(id) do:
	n' = myself;
	if (id !€ (n'.id, n'.successorId))
		nodeRef <- closest_preceding_finger(id)
		(nId,nRef) <- SendAndWait(FIND_PREDECESSOR, nodeRef, id)
	return (n'.id,n'ref)
   */
  private def find_predecessor(id: Long): (Long, ActorRef) = {   
    if (id < nodeId || id > successorId) {
      val (actorId: Long, actorRef: ActorRef) = closest_preceding_finger(id);
      implicit val timeout = Timeout(60 seconds);
      val future = actorRef ? FindPredecessorRequest(id);
      val result = Await.result(future, timeout.duration).asInstanceOf[FindPredecessorResponse];
      return (result.predecessorId, result.predecessorRef);
    }       
    return (nodeId, self);
    /*
    if (rid < id && successorId.exists(_ < id)) {
       val ref: ActorRef = closest_preceding_finger(rid);
       implicit val timeout = Timeout(5 seconds)
      val future: Future[FindSuccessorResponse] = ask(ref, FindSuccessorRequest(rid)).mapTo[FindSuccessorResponse]
      val result = Await.result(future, 1 second)
    */
  }
  
  /*
   // return closest finger preceding id
	Upon closest_preceding_finger(id) do:
	for(i<-m downto 1)
		if(finger[i].nodeId � (myself.id,id))
			return finger[i].nodeRef
	return myself;
   */
  private def closest_preceding_finger(id: Long): (Long, ActorRef) = {
    for (i <- (m-1) to 0) {
      if (fingerTable(i).nodeId >= nodeId && fingerTable(i).nodeId <= id) {
        return (fingerTable(i).nodeId, fingerTable(i).nodeRef);
      }
    }
    return (nodeId, self);
  }
  
  /*
  //initialize finger table of local node;
	// contactNode is an arbitrary node already in the network
	Upon init_finger_table do:
	finger[1].node <- trigger sendAndWait(FIND_SUCCESSOR, contactNode, finger[1].start)
	(predecessorId, predecessorRef) <- trigger sendAndWait(UPDATE_PREDECESSOR, successorRef, myself.id, myselfRef)
	for i = 1 to m - 1
		if (finger[i+ 1].startId � [myself.id, finger[i].nodeId))
			finger[i+ 1].nodeId <- finger[i].nodeId;
			finger[i+ 1].nodeRef <- finger[i].nodeRef;
		else
			(finger[i+ 1].nodeId, finger[i+ 1].nodeRef) <- sendAndWait(FIND_SUCCESSOR, contactNode, finger[i+1].start) 
  */
  private def init_finger_table() = {
    implicit val timeout = Timeout(60 seconds);
    val future = contactNode ? FindSuccessorRequest(fingerTable(0).startId);
    val result = Await.result(future, timeout.duration).asInstanceOf[FindSuccessorResponse];
    fingerTable(0).nodeId = result.successorId;
    fingerTable(0).nodeRef = result.successorRef;
      
    val future2 = result.successorRef ? UpdatePredecessorRequest(nodeId,self);
    val result2 = Await.result(future2, timeout.duration).asInstanceOf[UpdatePredecessorResponse];
    predecessorId = result2.oldPredecessorId;
    predecessorRef = result2.oldPredecessorRef;
    
    for (i<-0 to m-2) {
      if (fingerTable(i+1).startId >= nodeId && fingerTable(i+1).startId <= fingerTable(i).startId) {
        fingerTable(i+1).nodeId = fingerTable(i).nodeId;
        fingerTable(i+1).nodeRef = fingerTable(i).nodeRef;
      } else {
        val future3 = contactNode ? FindSuccessorRequest(fingerTable(i+1).startId);
        val result3 = Await.result(future3, timeout.duration).asInstanceOf[FindSuccessorResponse];
        fingerTable(i+1).nodeId = result3.successorId;
        fingerTable(i+1).nodeRef = result3.successorRef;
      }
    }
  }
  
  /*
   Upon join(nodeRef)
	predecessor <- nil
	(successorId,successorRef) <- SendAndWait(FIND_SUCCESSOR, nodeRef, myself.id)
   */
  private def join(nodeRef: ActorRef) = {
    predecessorId = 0;
    predecessorRef = null;
    implicit val timeout = Timeout(60 seconds);
    val future = contactNode ? FindSuccessorRequest(nodeId);
    val result = Await.result(future, timeout.duration).asInstanceOf[FindSuccessorResponse];
    successorId   = result.successorId;
    successorRef = result.successorRef;
  } 
  
  /*
  //periodically verify node's immediante successor and tell successor about it
	Upon stabilize do:
	(xId,xRef) <- trigger sendAndWait(GET_PREDECESSOR,successorRef)
	if xId € (myself.id,successorId)
		successorId <- xId
		successorRef <- xRef
	send(NOTIFY_SUCCESSOR, myself.id, myselfRef)
  */
  private def stabilize() = {
    implicit val timeout = Timeout(60 seconds);
    val future = successorRef ? GetPredecessorRequest;
    val result = Await.result(future, timeout.duration).asInstanceOf[GetPredecessorResponse];
    if (result.predecessorId >= nodeId && result.predecessorId <= successorId) {
      successorId = result.predecessorId;
      successorRef = result.predecessorRef;
    }
    val future2 = successorRef ? NotifySuccessorRequest(nodeId, self);
    val result2 = Await.result(future2, timeout.duration).asInstanceOf[NotifySuccessorResponse];
  } 
  
  /*
  //Node Node(nodeIf,nodeRef) thinks it might be our predecessor
	Upon notify(nodeId, nodeRef) do:
	if (predecessor is nil or nodeId � (predecessorId,myself.id)
		predecessorId <- nodeId
		predecessorRef <- nodeRef
  */
  private def notify(nId: Long, nRef: ActorRef) = {
    if (predecessorRef == null || (nId >= predecessorId && nId <= nodeId)) {
      predecessorId = nId;
      predecessorRef = nRef;
    }
   }  
  
  /*
  // called periodically. refreshes finger table entries.
  // next stores the index of the next finger to fix.
  Upon n.fix_fingers() do:
    next = next + 1;
    if (next > m)
    	next = 1;
    finger[next] = find_successor(n + 2^(next−1));
  */
  private def fix_fingers() = {  
    fix_fingers_next = fix_fingers_next + 1;
    if (fix_fingers_next > m) // To be confirmed...
      fix_fingers_next = 1;
    val (nId: Long, nRef: ActorRef) = find_successor(nodeId + Math.pow(2, fix_fingers_next - 1).toLong);
    fingerTable(fix_fingers_next).nodeId = nId;
    fingerTable(fix_fingers_next).nodeRef = nRef;
  }
  
  //called periodically. checks whether predecessor has failed.
  private def check_predecessor() = {
    implicit val timeout = Timeout(60 seconds);
    val future = predecessorRef ? HeartbeatRequest;
    try {
      val result = Await.result(future, timeout.duration).asInstanceOf[HeartbeatResponse]
    } catch {
      case e:TimeoutException => {
        predecessorId = 0;
        predecessorRef = null;
      }
      case e:Exception => {
        log.info("check_predecessor() throwed unknown exception: " + e.getMessage())
      }
    }
    
  }
  
  //let it receive requested key from master and return the result of search in finger table to caller
  private def sha1Hash(s: String): String = 
  {
    return MessageDigest.getInstance("SHA-1").digest(s.getBytes).toString
  }
  
  //TODO: reimplement response to each message case
  
  override def receive: Receive = {
    case FindSuccessorRequest(nId) =>
      val (rId, rRef) = find_successor(nId);
      sender() ! FindSuccessorResponse(rId, rRef) // return trigger self.find_successor(id)
    case FindPredecessorRequest(nId) =>
      val (rId, rRef) = find_predecessor(nId);
      sender() ! FindPredecessorResponse(rId, rRef) // return trigger self.find_predecessor(id)
    case UpdatePredecessorRequest(nId, nRef) =>
      /* 
      	(oldPredecessorId,oldPredecessorRef) <- (predecessorId, predecessorRef)
      	predecessorId <- nodeId
      	predecessorRef <- nodeRef
      	return (oldPredecessorId,oldPredecessorRef);
      */
      val oldPredecessorId = predecessorId;
      val oldPredecessorRef = predecessorRef;
      predecessorId = nId;
      predecessorRef = nRef;
      sender() ! UpdatePredecessorResponse(oldPredecessorId, oldPredecessorRef)
    case GetSuccessorRequest =>
      sender() ! GetSuccessorResponse(successorId,successorRef) // return (successorId, successorRef);
    case GetPredecessorRequest =>
      sender() ! GetPredecessorResponse(predecessorId,predecessorRef) // return (predecessorId, predecessorRef);
    case NotifySuccessorRequest(nId, nRef) =>
      notify(nId, nRef); //trigger notify(nodeId,nodeRef)
      sender() ! NotifySuccessorResponse
    case HeartbeatRequest =>
      sender() ! HeartbeatResponse
  }; 
}