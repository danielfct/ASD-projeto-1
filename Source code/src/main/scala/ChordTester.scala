import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.{HashMap, HashSet, Set}

import SubscribeChord._
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, PoisonPill, Terminated}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

object ChordTester {

  final case object CreateRing

  final case object Subscribe

  final case object Publish

  final case object ShowStats

  final case object DebugNodes

  case class RegisterEvent(from: Int, to: Int, msgType: String, topic: String, topicId: Int, message: String)

  case class RegisterDelivery(id: Int, topic: String, message: String)

}

class ChordTester(numMaxNodes: Int, numRequests: Int, nodeFailurePercentage: Float) extends Actor with ActorLogging {

  import ChordTester._

  val actorSystem = ActorSystem("PublishSubscribeChord")
  val r = new Random
  var currentNrMessages: Int = 0
  var currentNrFailedNodes: Int = 0
  var currentNrRequests: Int = 0
  var currentNrHops: Int = 0
  var currentNrPublishes: Int = 0
  val nodesAlive: mutable.HashMap[Int, ActorRef] = new mutable.HashMap[Int, ActorRef]()
  val ids: mutable.HashMap[ActorRef, Int] = new mutable.HashMap[ActorRef, Int]()
  var nodeFailureTask: Cancellable = _
  var publishTopicsTask: Cancellable = _
  var topicSubscribedBy = new HashMap[String, Set[Int]]
  var stats = new ConcurrentHashMap[String, ConcurrentMap[Int, Int]]
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

  def getRandomMessage: String = {
    val index = r.nextInt(messages.size)
    messages(index)
  }

  def getRandomTopic: String = {
    val index = r.nextInt(topics.size)
    topics(index)
  }

  def getRandomNode: ChordNode = {
    var id = -1
    do {
      id = r.nextInt(numMaxNodes)
    } while (!nodesAlive.contains(id))
    ChordNode(id, nodesAlive(id))
  }

  override def receive: Receive = {

    case CreateRing =>
      log.info("Creating nodes...")
      var id: Int = r.nextInt(numMaxNodes)
      val masterNode: ActorRef = actorSystem.actorOf(SubscribeChord.props(numMaxNodes), "NodeMaster")
      context.watch(masterNode)
      masterNode ! AddNode(id, masterNode)
      nodesAlive += (id -> masterNode)
      ids += (masterNode -> id)
      for (_ <- 0 until numMaxNodes - 1) {
        do {
          id = r.nextInt(numMaxNodes)
        } while (nodesAlive contains id)
        val chordNode: ActorRef = actorSystem.actorOf(SubscribeChord.props(numMaxNodes), "Node" + id)
        context.watch(chordNode)
        chordNode ! AddNode(id, masterNode)
        nodesAlive += (id -> chordNode)
        ids += (chordNode -> id)
      }
      log.info("Nodes created: " + nodesAlive.keySet.toString())
      if (nodeFailurePercentage > 0.0) {
        nodeFailureTask = context.system.scheduler.schedule(0 milliseconds, 1 second, getRandomNode.ref, PoisonPill)
      }

    case Subscribe =>
      log.info("Sending subscriptions...")
      for ((id, node) <- nodesAlive) {
        for (_ <- 0 until 5) {
          val topic = getRandomTopic
          node ! SendMessage(topic, "SUBSCRIBE", "")
          val map = new ConcurrentHashMap[Int, Int]
          map.put(id, -1)
          stats.put(topic, map)
          if (!topicSubscribedBy.containsKey(topic)) {
            topicSubscribedBy.put(topic, new HashSet[Int])
          }
          topicSubscribedBy.get(topic).add(id)
        }
      }

    case Publish =>
      log.info("Publishing...")
      val node = getRandomNode
      val topic = getRandomTopic
      val msg = getRandomMessage
      node.ref ! SendMessage(topic, "PUBLISH", msg)
      currentNrRequests += 1
      if (topicSubscribedBy.containsKey(topic)) {
        currentNrPublishes += topicSubscribedBy.get(topic).size()
      }

    case CountMessage => currentNrMessages += 1

    case ShowStats =>
      log.info("Stats:")
      log.info("  Num messages exchanged: {}", currentNrMessages)
      // log.info("Avg hops: {}", currentNrHops/currentNrRequests)       //TODO average hops
      log.info("  Num failed nodes: {}", currentNrFailedNodes)
      log.info("  M: {}", math.max(3, math.ceil(math.log(numMaxNodes) / math.log(2)).toInt))
      log.info("  Ring size: {}", numMaxNodes)
      log.info("  Num publishes requests: {}", currentNrRequests)

      var numSubscriptions = 0
      var numSuccessfulSubscriptions = 0
      var numPublishesDeliveries = 0
      stats.values().forEach(m => {
        m.values().forEach(seq => {
          if (seq >= -1)
            numSubscriptions += 1
          if (seq >= 0)
            numSuccessfulSubscriptions += 1
          if (seq > 0)
            numPublishesDeliveries += seq
        })
      })

      log.info("  Num subscriptions requests: {}", numSubscriptions)
      log.info("  Num successful subscriptions: {} ({}%)", numSuccessfulSubscriptions, (numSuccessfulSubscriptions / numSubscriptions) * 100)
      log.info("  Num publishes requests: {}", currentNrPublishes)
      log.info("  Num successful publishes deliveries: {} ({}%)", numPublishesDeliveries, (numPublishesDeliveries / currentNrPublishes) * 100)

    case RegisterEvent(from, to, msgType, topic, topicId, msg) =>
      if (msgType.equals("SUBSCRIBE")) {
        val map = stats.get(topic)
        map.put(from, 0)
        stats.put(topic, map)
      }

    case RegisterDelivery(id, topic, message) =>
      val map = stats.get(topic)
      if (map != null) {
        val mapValue = map.get(id)
        if (mapValue >= 0) {
          map.put(id, mapValue + 1)
          stats.put(topic, map)
        } else log.info("WARNING: GOT MESSAGE AND WASN'T EXPECTING FOR IT....")
      }

    case Terminated(actor: ActorRef) => {
      val id = ids(actor)
      nodesAlive -= id
      currentNrFailedNodes += 1
      print("node " + id + " failed")
      if (currentNrFailedNodes > numMaxNodes * nodeFailurePercentage && nodeFailureTask != null) {
        nodeFailureTask.cancel()
      }
    }

    case DebugNodes =>
      for ((_, node) <- nodesAlive) {
        node ! Debug
      }

  }

}