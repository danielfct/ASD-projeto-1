import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.HashMap
import java.util.Map
import java.security.MessageDigest
import java.nio.ByteBuffer
import java.util.List
import java.util.LinkedList

import SubscribeChord._

import scala.collection.mutable

object SubscribeChord {
  def props(numMaxNodes: Int): Props = Props(new SubscribeChord(numMaxNodes))

  case class ChordNode(id: Int, ref: ActorRef)

  case class addNode(id: Int, contactNode: ActorRef)

  case class findSuccessor(id: Int, node: ActorRef, message: Option[Message], hops: Int)

  case class foundSuccessor(successor: ChordNode)

  case object Stabilize

  case object AskPredecessor

  case class sendPredecessor(newSuccessor: ChordNode)

  case class notification(node: ChordNode)

  case object FixFingers

  case class findFingerSuccessor(index: Int, nodeId: Int, originalSender: ChordNode)

  case class sendFingerSuccessor(index: Int, originalSender: ChordNode)

  case class foundFingerSuccessor(index: Int, node: ChordNode)

  case object CheckPredecessor

  case object KeepAlive

  case object KeepAliveSignal

  case object KeepAliveReply

  case object Debug

  case class sendMessage(topic: String, msgType: String, msg: String)

  case class deliver(id: Int, message: Option[Message])

  case class messageDelivery(topic: String, message: String)

  case object RefreshMySubscriptions

  case object CheckMyTopicsSubscribersTTL

  class Message(var topic: String, var msgType: String, var msg: String, var originalId: Int, var originalRef: ActorRef)

  case object CountMessage

  case class registerEvent(from: Int, to: Int, msgType: String, topic: String, topicId: Int, message: String)

  case class registerDelivery(id: Int, topic: String, message: String)

  case object NodeFailure
}

class SubscribeChord(numMaxNodes: Int) extends Actor with ActorLogging {
  var m: Int = math.max(3, math.ceil(math.log(numMaxNodes / math.log(2))).toInt)
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

    case sendMessage(topic: String, msgType: String, msg: String) =>
      val m = new Message(topic, msgType, msg, selfNode.id, self)
      if (msgType.equals("SUBSCRIBE"))
        subscriptionsAlreadySent.add(m)
      else if (msgType.equals("UNSUBSCRIBE"))
        subscriptionsAlreadySent.remove(m)
      self ! findSuccessor(intSHA1Hash(topic), self, Some(m), 0)
      //self ! route(intSHA1Hash(topic), Some(m))

    case deliver(id, message) =>
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
                masterNode ! registerEvent(message.originalId,selfNode.id,"SUBSCRIBE",message.topic,id,"")
            case "UNSUBSCRIBE" =>
              val topicSubscribers = topicsWithSubscriptions.get(message.topic)
              val topicSubscribersTTL = subscriptionsTTL.get(message.topic)
              if (topicSubscribers != null) {
                topicSubscribers.remove(message.originalId)
                topicSubscribersTTL.remove(message.originalId)
              }

              masterNode ! registerEvent(message.originalId,selfNode.id,"UNSUBSCRIBE",message.topic,id,"")
            case "PUBLISH" =>
              val subscribers: Map[Integer, ActorRef] = topicsWithSubscriptions.get(message.topic)
              if (subscribers != null) {
                subscribers.values().forEach(sub => sub ! messageDelivery(message.topic, message.msg))
              }

              masterNode ! registerEvent(message.originalId,selfNode.id,"PUBLISH",message.topic,id,message.msg)
          }
          case None => log.info("Warning: route got an empty message!");
        }

    case messageDelivery(topic, message) =>
      masterNode ! registerDelivery(selfNode.id, topic, message)

    case findSuccessor(id, node, message, hops) =>
      if (isInInterval(id, selfNode.id, fingerTable(0).id, includeStart = false, includeEnd = true)) {
        message match {
          case None => node ! foundSuccessor(fingerTable(0))
          case message => fingerTable(0).ref ! deliver(id, message)
        }
      }
      else {
        masterNode ! CountMessage
        closestPrecedingFinger(id).ref ! findSuccessor(id, node, message, hops + 1)
      }

    case foundSuccessor(successor) =>
      fingerTable(0) = successor

    case addNode(id, contactNode) =>
      selfNode = ChordNode(id, self)
      masterNode = sender
      for (i <- 0 until m) {
        fingerTable(i) = selfNode
      }
      context.system.scheduler.schedule(0 milliseconds, 500 milliseconds, self, Stabilize) //TODO timer
      context.system.scheduler.schedule(0 milliseconds, 250 milliseconds, self, FixFingers) //TODO timer dependendo do tamanho m
      context.system.scheduler.schedule(7 seconds, 7 seconds, self, CheckPredecessor)
      context.system.scheduler.schedule(2 seconds, 2 seconds, self, KeepAlive)
      context.system.scheduler.schedule(5 seconds, 5 seconds, self, RefreshMySubscriptions)
      context.system.scheduler.schedule(15 seconds, 15 seconds, self, CheckMyTopicsSubscribersTTL)
      if (contactNode != selfNode.ref)
        contactNode ! findSuccessor(selfNode.id, selfNode.ref, None, 0)

    case Stabilize =>
      fingerTable(0).ref ! AskPredecessor

    case AskPredecessor =>
      sender ! sendPredecessor(predecessor)

    case sendPredecessor(newSuccessor) =>
      if (newSuccessor != null && isInInterval(newSuccessor.id, selfNode.id, fingerTable(0).id, includeStart = false, includeEnd = false)) {
        fingerTable(0) = newSuccessor
      }
      if (fingerTable(0) != selfNode) {
        fingerTable(0).ref ! notification(selfNode)
      }

    case notification(node) =>
      if (predecessor == null || isInInterval(node.id, predecessor.id, selfNode.id, includeStart = false, includeEnd = false)) {
        predecessor = node
      }

    case FixFingers =>
      val fingerId: Int = ((selfNode.id + Math.pow(2, nextFinger)) % Math.pow(2, m)).toInt
      selfNode.ref ! findFingerSuccessor(nextFinger, fingerId, selfNode)
      nextFinger = math.max(1, (nextFinger + 1) % m)

    case findFingerSuccessor(index, nodeId, originalSender) =>
      if (isInInterval(nodeId, selfNode.id, fingerTable(0).id, includeStart = false, includeEnd = true)) {
        fingerTable(0).ref ! sendFingerSuccessor(index, originalSender)
      }
      else {
        closestPrecedingFinger(nodeId).ref ! findFingerSuccessor(index, nodeId, originalSender)
      }

    case sendFingerSuccessor(index, originalSender) =>
      originalSender.ref ! foundFingerSuccessor(index, fingerTable(0))

    case foundFingerSuccessor(index: Int, node: ChordNode) =>
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
      subscriptionsAlreadySent.forEach(subscription => self ! findSuccessor(intSHA1Hash(subscription.topic), self, Some(subscription), 0))

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
      for (i <- 0 until m) {
        log.info("FingerTable[%d] = %d".format(i, fingerTable(i).id))
      }
      /*log.info("m: " + m + " --- from node " + selfKey)
      log.info("ringSize: " + ringSize + " --- from node " + selfKey)
      log.info("selfKey: " + selfKey + " --- from node " + selfKey)
      log.info("predecessorKey: " + predecessorKey + " --- from node " + selfKey)
      log.info("predecessorTTL: " + predecessorTTL + " --- from node " + selfKey)
      log.info("next: " + next + " --- from node " + selfKey)
      for (i <- 0 until m) {
        log.info("finger " + i + ":" + fingersKeys(i) + " --- from node " + selfKey)
      }
*/

    case NodeFailure =>
      context.stop(self)
  }
}
