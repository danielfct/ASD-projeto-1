
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, Terminated}

import scala.collection.mutable
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class ChordTester(numMaxNodes: Int, numRequests: Int, nodeFailurePercentage: Float) extends Actor with ActorLogging {
  val actorSystem = ActorSystem("PublishSubscribeChord")
  val r = new Random
  var currentNrMessages: Int = 0
  var currentNrFailedNodes: Int = 0
  val nodesAlive: mutable.HashMap[Int, ActorRef] = new mutable.HashMap[Int, ActorRef]()
  val ids: mutable.HashMap[ActorRef, Int] = new mutable.HashMap[ActorRef, Int]()

  // nó inicial
  var id: Int = r.nextInt(numMaxNodes)
  val actorInit: ActorRef = actorSystem.actorOf(SubscribeChord.props(self), "Initializer")
  context.watch(actorInit)
  actorInit ! create(numMaxNodes, id, actorInit)
  nodesAlive + (id -> actorInit)
  ids + (actorInit -> id)

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

  val messageTypes: List[String] = List("SUBSCRIBE", "PUBLISH", "UNSUBSCRIBE")
  val topics: List[String] = List("Health Care", "Consumer Services", "Energy", "Finance", "Basic Industries",
    "Technology", "Transportation", "Miscellaneous", "Capital Goods", "Public Utilities", "Consumer Durables",
    "Electric Utilities: Central", "Industrial Machinery/Components", "Oil & Gas Production", "Power Generation",
    "Savings Institutions", "Restaurants", "Food Chains", "Natural Gas Distribution", "Packaged Foods",
    "Biotechnology: Biological Products", "Metal Fabrications", "Computer Software: Prepackaged Software",
    "Investment Bankers/Brokers/Service", "RETAIL: Building Materials", "Real Estate Investment Trusts", "Major Banks",
    "Major Pharmaceuticals", "Advertising", "Semiconductors", "Biotechnology: Laboratory Analytical Instruments",
    "Telecommunications Equipment", "Television Services", "Other Specialty Stores", "Specialty Insurers",
    "Consumer Specialties", "Electronic Components", "Home Furnishings", "Package Goods/Cosmetics", "Commercial Banks",
    "Environmental Services", "Auto Parts:O.E.M.", "EDP Services", "Miscellaneous manufacturing industries",
    "Hotels/Resorts", "Ophthalmic Goods", "Business Services", "Precious Metals", "Consumer Electronics/Appliances",
    "Major Chemicals", "Oil Refining/Marketing", "Marine Transportation", "Hospital/Nursing Management")
  val messages: List[String] = List(
    "in purus eu magna vulputate luctus cum sociis natoque penatibus et magnis dis parturient",
    "semper sapien a libero nam dui proin leo odio porttitor id consequat in consequat ut nulla sed accumsan",
    "varius integer ac leo pellentesque ultrices mattis odio donec vitae",
    "et ultrices posuere cubilia curae nulla dapibus dolor vel est donec odio justo sollicitudin ut",
    "amet sapien dignissim vestibulum vestibulum ante ipsum primis in faucibus orci",
    "sit amet sapien dignissim vestibulum vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere")

  var nodeFailureTask: Cancellable = _
  if (nodeFailurePercentage > 0.0) {
    nodeFailureTask = context.system.scheduler.schedule(0 milliseconds, 50 milliseconds, getRandomNode, NodeFailure)
  }

  for (_ <- 0 until numRequests) {
    val randomNode: ActorRef = getRandomNode
    val randomTopic: String = getRandomTopic
    val randomMessageType: String = getRandomMessageType
    var randomMessage: String = ""
    if (randomMessageType.equals("PUBLISH")) {
      randomMessage = getRandomMessage
    }
    randomNode ! sendMessage(randomTopic, randomMessageType, randomMessage)
  }

  def getRandomMessageType: String = {
    val index = r.nextInt(messageTypes.size)
    messageTypes(index)
  }

  def getRandomMessage: String = {
    val index = r.nextInt(messages.size)
    messages(index)
  }

  def getRandomTopic: String = {
    val index = r.nextInt(topics.size)
    topics(index)
  }

  def getRandomNode: ActorRef = {
    var id = -1
    do {
      id = r.nextInt(numMaxNodes)
    } while (!nodesAlive.contains(id))
    nodesAlive(id)
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
  }

  println("Current number or failed nodes: " + currentNrFailedNodes + " (" + (currentNrFailedNodes/numMaxNodes).toDouble + "%)")
  println("Total number of messages: " + currentNrMessages + " (" + (currentNrMessages/numRequests).toDouble + "%)")

}