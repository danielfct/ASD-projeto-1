import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.{HashMap, LinkedList, Map}

import ChordTester.{RegisterDelivery, RegisterEvent}
import SubscribeChord._
import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object SubscribeChord {
  def props(numMaxNodes: Int): Props = Props(new SubscribeChord(numMaxNodes))

  case class ChordNode(id: Int, ref: ActorRef)

  class Message(var topic: String, var msgType: String, var msg: String, var originalId: Int, var originalRef: ActorRef)

  case class AddNode(id: Int, contactNode: ActorRef)

  case class FindSuccessor(id: Int, node: ActorRef, message: Option[Message], hops: Int)

  case class FoundSuccessor(successor: ChordNode)

  case object Stabilize

  case object AskPredecessor

  case class SendPredecessor(newSuccessor: ChordNode)

  case class Notification(node: ChordNode)

  case object FixFingers

  case class FindFingerSuccessor(index: Int, nodeId: Int, originalSender: ChordNode)

  case class FoundFingerSuccessor(index: Int, node: ChordNode)

  case object CheckPredecessor

  case object KeepAlive

  case object KeepAliveSignal

  case object KeepAliveReply

  case object RefreshMySubscriptions

  case object CheckMyTopicsSubscribersTTL

  case class SendMessage(topic: String, msgType: String, msg: String)

  case class Deliver(id: Int, message: Option[Message], hops: Int)

  case class MessageDelivery(topic: String, message: String)

  case object CountMessage

  case object Debug

}

class SubscribeChord(numMaxNodes: Int) extends Actor with ActorLogging {
  var m: Int = math.max(3, math.ceil(math.log(numMaxNodes) / math.log(2)).toInt)
  var ringSize: Int = numMaxNodes
  var masterNode: ActorRef = _
  var selfNode: ChordNode = _
  var fingerTable = new Array[ChordNode](m)
  var nextFinger: Int = 1
  var predecessor: ChordNode = _
  var predecessorTTL: Long = 0
  val TTL: Int = 15000
  var topicsWithSubscriptions = new HashMap[String, Map[Integer, ActorRef]]
  var subscriptionsAlreadySent = new LinkedList[Message]
  var subscriptionsTTL = new HashMap[String, Map[Integer, Long]]


  def isInInterval(value: Int, start: Int, end: Int, includeStart: Boolean, includeEnd: Boolean): Boolean = {
    if (start == end && value != start && includeStart != includeEnd) {
      return true
    }
    if (includeStart) {
      if (includeEnd) { // [start,end]
        if (start <= end) value >= start && value <= end else value >= start || value <= end
      } else { // [start,end[
        if (start < end) value >= start && value < end else value >= start || value < end
      }
    } else if (includeEnd) { // ]start,end]
      if (start < end) value > start && value <= end else value > start || value <= end
    } else { // ]start,end[
      if (start < end) value > start && value < end else value > start || value < end
    }
  }

  def closestPrecedingFinger(id: Int): ChordNode = {
    for (i <- (m - 1) to 0 by -1) {
      if (isInInterval(fingerTable(i).id, selfNode.id, id, includeStart = false, includeEnd = false)) {
        return fingerTable(i)
      }
    }
    selfNode
  }

  def intSHA1Hash(topic: String): Int = {
    val sha1Bytes = MessageDigest.getInstance("SHA-1").digest(topic.getBytes)
    Math.abs(ByteBuffer.wrap(sha1Bytes).getInt % ringSize)
  }

  override def receive = {

    case SendMessage(topic: String, msgType: String, msg: String) =>
      val m = new Message(topic, msgType, msg, selfNode.id, self)
      if (msgType.equals("SUBSCRIBE"))
        subscriptionsAlreadySent.add(m)
      else if (msgType.equals("UNSUBSCRIBE"))
        subscriptionsAlreadySent.remove(m)
      self ! FindSuccessor(intSHA1Hash(topic), self, Some(m), 0)
    //self ! route(intSHA1Hash(topic), Some(m))

    case Deliver(id, message, hops) =>
      message match {
        case Some(message) => message.msgType match {
          case "SUBSCRIBE" =>
            var topicSubscribers = topicsWithSubscriptions.get(message.topic)
            var topicSubscribersTTL = subscriptionsTTL.get(message.topic)
            if (topicSubscribers == null) {
              topicsWithSubscriptions.put(message.topic, new HashMap[Integer, ActorRef]())
              topicSubscribers = topicsWithSubscriptions.get(message.topic)
              subscriptionsTTL.put(message.topic, new HashMap[Integer, Long]())
              topicSubscribersTTL = subscriptionsTTL.get(message.topic)
            }
            val isRefresh = topicSubscribers.containsKey(message.originalId)
            topicSubscribers.put(message.originalId, message.originalRef)
            topicSubscribersTTL.put(message.originalId, System.currentTimeMillis())
            if (!isRefresh)
              masterNode ! RegisterEvent(message.originalId, selfNode.id, "SUBSCRIBE", message.topic, id, "")

          case "UNSUBSCRIBE" =>
            val topicSubscribers = topicsWithSubscriptions.get(message.topic)
            val topicSubscribersTTL = subscriptionsTTL.get(message.topic)
            if (topicSubscribers != null) {
              topicSubscribers.remove(message.originalId)
              topicSubscribersTTL.remove(message.originalId)
            }
            masterNode ! RegisterEvent(message.originalId, selfNode.id, "UNSUBSCRIBE", message.topic, id, "")

          case "PUBLISH" =>
            val subscribers: Map[Integer, ActorRef] = topicsWithSubscriptions.get(message.topic)
            if (subscribers != null) {
              subscribers.values().forEach(sub => sub ! MessageDelivery(message.topic, message.msg))
            }
            masterNode ! RegisterEvent(message.originalId, selfNode.id, "PUBLISH", message.topic, id, message.msg)

        }
        case None => log.info("Warning: route got an empty message!");
      }

    case MessageDelivery(topic, message) =>
      masterNode ! RegisterDelivery(selfNode.id, topic, message)

    case FindSuccessor(id, node, message, hops) =>
      if (isInInterval(id, selfNode.id, fingerTable(0).id, includeStart = false, includeEnd = true)) {
        message match {
          case None => node ! FoundSuccessor(fingerTable(0))
          case message => fingerTable(0).ref ! Deliver(id, message, hops)
        }
      }
      else {
        masterNode ! CountMessage
        closestPrecedingFinger(id).ref ! FindSuccessor(id, node, message, hops + 1)
      }

    case FoundSuccessor(successor) =>
      fingerTable(0) = successor

    case AddNode(id, contactNode) =>
      selfNode = ChordNode(id, self)
      masterNode = sender
      for (i <- 0 until m) {
        fingerTable(i) = selfNode
      }
      context.system.scheduler.schedule(0 milliseconds, 50 milliseconds, self, Stabilize) //TODO timer
      context.system.scheduler.schedule(0 milliseconds, 50 milliseconds, self, FixFingers) //TODO timer dependendo do tamanho m
      context.system.scheduler.schedule(7 seconds, 7 seconds, self, CheckPredecessor)
      context.system.scheduler.schedule(2 seconds, 2 seconds, self, KeepAlive)
      context.system.scheduler.schedule(5 seconds, 5 seconds, self, RefreshMySubscriptions)
      context.system.scheduler.schedule(15 seconds, 15 seconds, self, CheckMyTopicsSubscribersTTL)
      if (contactNode != selfNode.ref)
        contactNode ! FindSuccessor(selfNode.id, selfNode.ref, None, 0)

    case Stabilize =>
      fingerTable(0).ref ! AskPredecessor

    case AskPredecessor =>
      sender ! SendPredecessor(predecessor)

    case SendPredecessor(newSuccessor) =>
      if (newSuccessor != null && isInInterval(newSuccessor.id, selfNode.id, fingerTable(0).id, includeStart = false, includeEnd = false)) {
        fingerTable(0) = newSuccessor
      }
      if (fingerTable(0) != selfNode) {
        fingerTable(0).ref ! Notification(selfNode)
      }

    case Notification(node) =>
      if (predecessor == null || isInInterval(node.id, predecessor.id, selfNode.id, includeStart = false, includeEnd = false)) {
        predecessor = node
      }

    case FixFingers =>
      val fingerId: Int = ((selfNode.id + Math.pow(2, nextFinger)) % numMaxNodes).toInt
      selfNode.ref ! FindFingerSuccessor(nextFinger, fingerId, selfNode)
      nextFinger = (nextFinger + 1) % m

    case FindFingerSuccessor(index, nodeId, originalSender) =>
      if (isInInterval(nodeId, selfNode.id, fingerTable(0).id, includeStart = false, includeEnd = true)) {
        originalSender.ref ! FoundFingerSuccessor(index, fingerTable(0))
      }
      else {
        closestPrecedingFinger(nodeId).ref ! FindFingerSuccessor(index, nodeId, originalSender)
      }

    case FoundFingerSuccessor(index: Int, node: ChordNode) =>
      fingerTable(index) = node

    case CheckPredecessor =>
      if (System.currentTimeMillis() > predecessorTTL) {
        predecessor = null
      }

    case KeepAlive =>
      if (predecessor != null) {
        predecessor.ref ! KeepAliveSignal
      }

    case KeepAliveSignal =>
      sender ! KeepAliveReply

    case KeepAliveReply =>
      predecessorTTL = System.currentTimeMillis() + TTL

    case RefreshMySubscriptions =>
      subscriptionsAlreadySent.forEach(subscription => self ! FindSuccessor(intSHA1Hash(subscription.topic), self, Some(subscription), 0))

    case CheckMyTopicsSubscribersTTL =>
      subscriptionsTTL.keySet().forEach(topic => {
        val subscriptionsTTLEntry = subscriptionsTTL.get(topic)
        val topicsWithSubscriptionsEntry = topicsWithSubscriptions.get(topic)
        val it: java.util.Iterator[Integer] = subscriptionsTTLEntry.keySet().iterator()
        while (it.hasNext) {
          val subscriberId: Integer = it.next()
          if (System.currentTimeMillis() > subscriptionsTTLEntry.get(subscriberId) + TTL) {
            it.remove()
            topicsWithSubscriptionsEntry.remove(subscriberId)
          }
        }
      })

    case Debug =>
      log.info("Ring size = %d".format(ringSize))
      log.info("FingerTable size = %d".format(m))
      log.info("Predecessor = %d".format(if (predecessor == null) -1 else predecessor.id))
      log.info("PredecessorTTL = %d".format(predecessorTTL - System.currentTimeMillis()))
      log.info("NextFinger = %d".format(nextFinger))
      for (i <- 0 until m) {
        log.info("FingerTable[%d] = %d".format(i, fingerTable(i).id))
      }

  }
}
