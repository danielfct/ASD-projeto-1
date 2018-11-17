
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, Terminated}

import scala.collection.mutable
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.LinkedList
import java.io.FileWriter

object ChordTester {
  final case object Subscribe
  final case object Publish
  final case object WriteResults
}

class Node(var id: Int, var ref: ActorRef)

class ChordTester(numMaxNodes: Int, numRequests: Int, nodeFailurePercentage: Float) extends Actor with ActorLogging {
  import ChordTester._
  
  val actorSystem = ActorSystem("PublishSubscribeChord")
  val r = new Random
  var currentNrMessages: Int = 0
  var currentNrFailedNodes: Int = 0
  var currentNrRequests : Int = 0
  val nodesAlive: mutable.HashMap[Int, ActorRef] = new mutable.HashMap[Int, ActorRef]()
  val ids: mutable.HashMap[ActorRef, Int] = new mutable.HashMap[ActorRef, Int]()
  var logOfEvents = new LinkedList[String]

  // nó inicial
  var id: Int = r.nextInt(numMaxNodes)
  val actorInit: ActorRef = actorSystem.actorOf(SubscribeChord.props(self), "Initializer")
  context.watch(actorInit)
  actorInit ! create(numMaxNodes, id, actorInit)
  nodesAlive += (id -> actorInit)
  ids += (actorInit -> id)

  log.info("Creating nodes...")
  // criação dos nós
  for (_ <- 0 until numMaxNodes-1) { //-1 para excluir o nó inicial
    do {
      id = r.nextInt(numMaxNodes)
    } while (nodesAlive contains id)
    val chordNode: ActorRef = actorSystem.actorOf(SubscribeChord.props(self), "Node" + id)
    context.watch(chordNode)
    chordNode ! create(numMaxNodes, id, actorInit)
    nodesAlive += (id -> chordNode)
    ids += (chordNode -> id)
  }
  
  log.info("nodes: " + nodesAlive.keySet.toString())
  
  context.system.scheduler.scheduleOnce(15 seconds, self, Subscribe)

  //val messageTypes: List[String] = List("SUBSCRIBE", "PUBLISH", "UNSUBSCRIBE")
  val topics: List[String] = List("Health Care", "Consumer Services", "Energy", "Finance", "Basic Industries",
    "Technology", "Transportation", "Miscellaneous", "Capital Goods", "Public Utilities", "Consumer Durables",
    "Electric Utilities", "Industrial Machinery", "Gas Production", "Power Generation",
    "Savings Institutions", "Restaurants", "Food Chains", "Natural Gas Distribution", "Packaged Foods",
    "Biotechnology", "Metal Fabrications", "Computer Software",
    "Investment Bankers", "Building Materials", "Real Estate Investment Trusts", "Major Banks",
    "Major Pharmaceuticals", "Advertising", "Semiconductors", "Laboratory Instruments",
    "Telecommunications Equipment", "Television Services", "Other Specialty Stores", "Specialty Insurers",
    "Consumer Specialties", "Electronic Components", "Home Furnishings", "Package Goods", "Commercial Banks",
    "Environmental Services", "Auto Parts", "EDP Services", "Miscellaneous manufacturing industries",
    "Hotels", "Ophthalmic Goods", "Business Services", "Precious Metals", "Consumer Electronics",
    "Major Chemicals", "Oil Refining", "Marine Transportation", "Hospital")
  val messages: List[String] = List(
    "in purus eu magna vulputate luctus cum sociis natoque penatibus et magnis dis parturient",
    "semper sapien a libero nam dui proin leo odio porttitor id consequat in consequat ut nulla sed accumsan",
    "varius integer ac leo pellentesque ultrices mattis odio donec vitae",
    "et ultrices posuere cubilia curae nulla dapibus dolor vel est donec odio justo sollicitudin ut",
    "amet sapien dignissim vestibulum vestibulum ante ipsum primis in faucibus orci",
    "sit amet sapien dignissim vestibulum vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere")

  var nodeFailureTask: Cancellable = _
  if (nodeFailurePercentage > 0.0) {
    nodeFailureTask = context.system.scheduler.schedule(0 milliseconds, 50 milliseconds, getRandomNode.ref, NodeFailure)
  }

  var publishTopicsTask: Cancellable = _
  
  /*for (_ <- 0 until numRequests) {
    val randomNode: ActorRef = getRandomNode
    val randomTopic: String = getRandomTopic
    val randomMessageType: String = getRandomMessageType
    var randomMessage: String = ""
    if (randomMessageType.equals("PUBLISH")) {
      randomMessage = getRandomMessage
    }
    randomNode ! sendMessage(randomTopic, randomMessageType, randomMessage)
  }*/

  /*def getRandomMessageType: String = {
    val index = r.nextInt(messageTypes.size)
    messageTypes(index)
  }*/

  def getRandomMessage: String = {
    val index = r.nextInt(messages.size)
    messages(index)
  }

  def getRandomTopic: String = {
    val index = r.nextInt(topics.size)
    topics(index)
  }

  def getRandomNode: Node = {
    var id = -1
    do {
      id = r.nextInt(numMaxNodes)
    } while (!nodesAlive.contains(id))
    return new Node(id,nodesAlive(id))
  }
  
  def initSubscriptions: Unit = {
    log.info("Nodes created. Sending subscriptions...")
    for ((id,node) <- nodesAlive) {
      for (i<-1 to 5) {
        val topic = getRandomTopic
        node ! sendMessage(topic, "SUBSCRIBE", "")
        addEventToLog(id,-1,"SUBSCRIBE",topic,-1,"")
      }
    }
    context.system.scheduler.schedule(0 milliseconds, 5 seconds, self, WriteResults)
    publishTopicsTask = context.system.scheduler.schedule(0 milliseconds, 2 seconds, self, Publish)
  }
  
  def addEventToLog(from: Int, to: Int, msgType: String, topic: String, topicId: Int, msg: String) : Unit = {
    var event: String = "";
    if (topicId != -1 && to != -1)
      event = "<" + from + "," + to + "," + msgType + "," + topic + "," + topicId + "," + msg + ">";
    else
     event = "<" + from + "," + msgType + "," + topic + "," + msg + ">"
    logOfEvents.add(event)
  }
  
  def writeResultsToFile : Unit = {
    log.info("Writing results...")
    
    val fw = new FileWriter("results.txt", true)
    var it = logOfEvents.iterator()
    try {
      while(it.hasNext()) {
        fw.write(it.next()+"\n")
        it.remove()
      }
    } finally fw.close()

  }
    

  override def receive: Receive = {
    case Terminated(actor: ActorRef) => {
      val id = ids(actor)
      nodesAlive -= id
      currentNrFailedNodes += 1
      print("node " + id + " failed")
      if (currentNrFailedNodes > numMaxNodes * nodeFailurePercentage && nodeFailureTask != null) {
        nodeFailureTask.cancel()
      }
    }
    
    case CountMessage => currentNrMessages += 1
    
    case Publish => 
      if (currentNrRequests <= numRequests) {
        log.info("Publishing...")
          val node = getRandomNode
          val topic = getRandomTopic
          val msg = getRandomMessage
          node.ref ! sendMessage(topic, "PUBLISH", msg)
          addEventToLog(node.id,-1,"PUBLISH",topic,-1,msg)
          currentNrRequests += 1
      } else {
        publishTopicsTask.cancel()
        log.info("Stopped publishing...")
        }
      
    case Subscribe => initSubscriptions
    
    case registerEvent(from, to, msgType, topic, topicId, msg) =>
      addEventToLog(from,to,msgType,topic,topicId,msg)
     
    case registerDelivery(id, message) =>
      logOfEvents.add("<"+id+","+message+">")
      
    case WriteResults => writeResultsToFile
      
  }

  println("Current number or failed nodes: " + currentNrFailedNodes + " (" + (currentNrFailedNodes/numMaxNodes).toDouble + "%)")
  println("Total number of messages: " + currentNrMessages + " (" + (currentNrMessages/numRequests).toDouble + "%)")

}