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
  final case class GetPredecessorRequest();
  final case class GetPredecessorResponse(predecessorId: Long, predecessorRef: ActorRef);
  final case class UpdateFingerTableRequest(nodeId: Long, nodeRef: ActorRef, i: Int);
  final case class UpdateFingerTableResponse(); // ???
  final case class NotifySuccessorRequest(nodeId: Long, nodeRef: ActorRef);
  final case class NotifySuccessorResponse(); // ???
  
}

class FingerEntry(var startId: Long, var nodeId: Long, var nodeRef: ActorRef);

class ChordNode(id: Long, nodeCount: Int, contactNode: ActorRef) extends Actor
{
  import ChordNode._
  var numNodes: Int = nodeCount;
  var nodeId: Long = id;
  var successorId: Long = 0;  
  var successorRef: ActorRef = null;
  var predecessorId: Long = 0;
  var predecessorRef: ActorRef = null;
  
  var hopCount:Int = 0;
  
  //get hash of nodeId as an array of Byte
  var hashResult: String = sha1Hash(nodeId.toString);
  
  //construct a finger table for this node instance
  val m: Int = (math.log(numNodes)/math.log(2)).toInt;
  var fingerTable = new Array[FingerEntry](m);
  
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
  //update all nodes whose finger tables should refer to n
	Upon update_others() do:
	for i<-1 to m
	//find last node p whose ith finger might be myself
	(pid,pref) <- tigger find_predecessor(myself.id - 2^(i-1))
	send(UPDATE_FINGER_TABLE,pref,myself.id,myselfRef,i)
  */
  private def update_others() = {
    for (i<-0 to m-1) {
      val (_, pRef: ActorRef) = find_predecessor(nodeId - Math.pow(2,i-1).toLong);
      pRef ! UpdateFingerTableRequest(nodeId, self, i);
    }
  }
  
  /*
  //if node(sId,sRef) is ith finger of myself, update my finger table with that node
	Upon update_finger_table(sId,sRef,i) do:
	if sId € (myself.id, finger[i].nodeId)
		finger[i].nodeId = sId
		finger[i].nodeRef = sRef
		send(UPDATE_FINGER_TABLE,predecessor,sId,sRef,i) // update predecessor finger table
  */
  private def update_finger_table(sId: Long, sRef: ActorRef, i: Int) = {
    if (sId >= nodeId && sId <= fingerTable(i).nodeId) {
      fingerTable(i).nodeId = sId;
      fingerTable(i).nodeRef = sRef;
      predecessorRef ! UpdateFingerTableRequest(sId, sRef, i);
    }
  }  
  
  /*
   Upon join(nodeRef)
	predecessor <- nil
	(successorId,successorRef) <- SendAndWait(FIND_SUCCESSOR, nodeRef, myself.id)
   */
  private def join(nodeRef: ActorRef) = {
    //TODO
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
    //TODO
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
  //periodically refresh finger table entries
	Upon fix_fingers() do:
	i = random index > 1 into finger[];
	finger[i].node <- trigger find_successor(finger[i].startId)
  */
  private def fix_fingers() = {
    val i = Random.nextInt(m-2) + 1; //?? NOT SURE!! Paper: i = random index > 1 into finger[]
    val (nId: Long, nRef: ActorRef) = find_successor(fingerTable(i).startId);
    fingerTable(i).nodeId = nId;
    fingerTable(i).nodeRef = nRef;
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
    case UpdateFingerTableRequest(nId, nRef, i) =>
      update_finger_table(nId,nRef,i);
      sender() ! UpdateFingerTableResponse // trigger update_finger_table(nodeId,nodeRef,i)
    case NotifySuccessorRequest(nId, nRef) =>
      notify(nId, nRef); //trigger notify(nodeId,nodeRef)
      sender() ! NotifySuccessorResponse
  }; 
}