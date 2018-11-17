import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.HashMap
import java.util.Map
import java.security.MessageDigest
import java.nio.ByteBuffer
import java.util.List
import java.util.LinkedList
import akka.actor.Props

object SubscribeChord {
  def props(tester: ActorRef): Props = Props(new SubscribeChord(tester))
}

class SubscribeChord(tester: ActorRef) extends Actor with ActorLogging {
  var m: Int = 0
  var ringSize: Int = 0
  var selfKey: Int = 0
  var predecessorRef: ActorRef = _
  var predecessorKey: Int = 0
  var predecessorTTL: Long = 0
  var fingersRefs = new Array[ActorRef](1)
  var fingersKeys = new Array[Int](1)
  var next: Int = 0
  var topicsWithSubscriptions = new HashMap[String, Map[Integer, ActorRef]]
  var subscriptionsAlreadySent = new LinkedList[Message]
  var subscriptionsTTL = new HashMap[String, Map[Integer, Long]]
  val TTL = 15000

  def isInInterval(value: Int, start: Int, end: Int, includeStart: Boolean, includeEnd: Boolean): Boolean = {
    //log.info("{}: isInInterval value={}, start={}, end={}", selfKey, value, start, end);
    //log.info("{}: isInInterval includeStart={}, includeEnd={}", selfKey, includeStart, includeEnd);
    if (start == end && value != start && includeStart != includeEnd) {
      //log.info("result true={}", res)
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

  def closest_preceding_finger(id: Int): ActorRef = {
    for (i <- (m - 1) to 0 by -1) {
      //log.info("index: {}", i)
      if (isInInterval(fingersKeys(i), selfKey, id, includeStart = false, includeEnd = false)) {
        //log.info("closest_preceding_finger: " + i)
        return fingersRefs(i)
      }
    }
    //log.info("closest_preceding_finger: " + -1)
    self
  }

  def intSHA1Hash(topic: String): Int = {
    val sha1Bytes = MessageDigest.getInstance("SHA-1").digest(topic.getBytes)
    math.abs(ByteBuffer.wrap(sha1Bytes).getInt % ringSize)
  }

  override def receive = {

    case sendMessage(topic: String, msgType: String, msg: String) =>
      val m = new Message(topic, msgType, msg, selfKey, self)
      if (msgType.equals("SUBSCRIBE"))
        subscriptionsAlreadySent.add(m)
      else if (msgType.equals("UNSUBSCRIBE"))
        subscriptionsAlreadySent.remove(m)
      self ! find_successor(intSHA1Hash(topic), self, Some(m))
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
              var isRefresh = topicSubscribers.containsKey(message.originalId)
              topicSubscribers.put(message.originalId, message.originalRef)
              topicSubscribersTTL.put(message.originalId, System.currentTimeMillis())
              
              if (!isRefresh) tester ! registerEvent(message.originalId,selfKey,"SUBSCRIBE",message.topic,id,"")
            case "UNSUBSCRIBE" =>
              val topicSubscribers = topicsWithSubscriptions.get(message.topic)
              val topicSubscribersTTL = subscriptionsTTL.get(message.topic)
              if (topicSubscribers != null) {
                topicSubscribers.remove(message.originalId)
                topicSubscribersTTL.remove(message.originalId)
              }
              
              tester ! registerEvent(message.originalId,selfKey,"UNSUBSCRIBE",message.topic,id,"")
            case "PUBLISH" =>
              val subscribers: Map[Integer, ActorRef] = topicsWithSubscriptions.get(message.topic)
              if (subscribers != null) {
                subscribers.values().forEach(sub => sub ! messageDelivery(message.msg))
              }
              
              tester ! registerEvent(message.originalId,selfKey,"PUBLISH",message.topic,id,message.msg)
          }
          case None => log.info("Warning: route got an empty message!");
        }
      


    case messageDelivery(message) =>
      log.info("{} got message = {}", selfKey, message)
      tester ! registerDelivery(selfKey, message)

    case find_successor(id, node, message) =>
      if (isInInterval(id, selfKey, fingersKeys(0), includeStart = false, includeEnd = true)) {
        //log.info("find_successor on self node " + selfKey + " with id " + id + " result: " + fingersKeys(0))
        message match {
          case None => node ! found_successor(fingersKeys(0), fingersRefs(0))
          case message => fingersRefs(0) ! deliver(id, message)
        }
      }
      else {
        //log.info("find_successor on self node " + selfKey + " with id " + id + " result: continue")
        closest_preceding_finger(id) ! find_successor(id, node, message)
      }

    case found_successor(id, node) =>
      fingersRefs(0) = node
      fingersKeys(0) = id

    //log.info("found_successor on self node " + selfKey + " found: " + id)

    case create(maxNumNodes, id, contactNode) =>
      m = math.max(3, math.ceil(math.log(maxNumNodes / math.log(2))).toInt)
      ringSize = maxNumNodes
      selfKey = id
      fingersRefs = new Array[ActorRef](m)
      fingersKeys = new Array[Int](m)
      predecessorRef = null
      predecessorKey = -1
      for (i <- 0 until m - 1) {
        fingersRefs(i) = self
        fingersKeys(i) = selfKey
      }
      context.system.scheduler.schedule(0 milliseconds, 5 milliseconds, self, stabilize())
      context.system.scheduler.schedule(0 milliseconds, 6 milliseconds, self, fix_fingers())
      context.system.scheduler.schedule(0 milliseconds, 7 seconds, self, check_predecessor())
      context.system.scheduler.schedule(0 milliseconds, 2 seconds, self, keepAlive())
      context.system.scheduler.schedule(0 milliseconds, 5 seconds, self, refreshMySubscriptions())
      context.system.scheduler.schedule(0 milliseconds, 15 seconds, self, checkMyTopicsSubscribersTTL())
      //log.info("create self node: " + selfKey)
      if (contactNode != null && contactNode != self)
        self ! initJoin(contactNode)

    case initJoin(contactNode) =>
      predecessorRef = null
      predecessorKey = -1
      //log.info("initJoin on self node " + selfKey)
      contactNode ! join(selfKey, self)

    case join(id, node) =>
      //log.info("join on self node " + selfKey + " with node id " + id)
      self ! find_successor(id, node, None)

    case stabilize() =>
      if (fingersRefs(0) != null) {
        //log.info("stabilize on self node " + selfKey)
        fingersRefs(0) ! stabilizeAskSuccessorPredecessor()
      }

    case stabilizeAskSuccessorPredecessor() =>
      //log.info("stabilizeAskSuccessorPredecessor on self node " + selfKey)
      sender() ! stabilizeSendSuccessorPredecessor(predecessorKey, predecessorRef)

    case stabilizeSendSuccessorPredecessor(id, node) =>
      if (id != -1 && isInInterval(id, selfKey, fingersKeys(0), includeStart = false, includeEnd = false)) {
        fingersRefs(0) = node
        fingersKeys(0) = id
        //log.info("stabilizeSendSuccessorPredecessor on self node " + selfKey + " finds successor: " + id)
      }
      if (fingersRefs(0) != self) {
        //log.info("start notification on self node " + selfKey)
        fingersRefs(0) ! notification(selfKey, self)
      }

    case notification(id, node) =>
      if (predecessorKey == -1 || isInInterval(id, predecessorKey, selfKey, includeStart = false, includeEnd = false)) {
        predecessorRef = node
        predecessorKey = id
        //log.info("notification on self node " + selfKey + " ok, my predecessor is: " + id)
      }

    case fix_fingers() =>
      next = next + 1
      //log.info("fix_fingers on self node " + selfKey + " with next value of: " + next)
      if (next > (m - 1)) {
        next = 0
      }
      self ! find_finger_successor(next, selfKey + Math.pow(2, next).toInt, self)

    case find_finger_successor(index, id, node) =>
      if (isInInterval(id, selfKey, fingersKeys(0), includeStart = false, includeEnd = true)) {
        //log.info("find_finger_successor on self node " + selfKey + " found for index " + index + " and id: " + fingersKeys(0))
        node ! found_finger_successor(index, fingersKeys(0), fingersRefs(0))
      }
      else {
        //log.info("find_finger_successor on self node " + selfKey + " found for index " + index + " and id: continue")
        closest_preceding_finger(id) ! find_finger_successor(index, id, node)
      }

    case found_finger_successor(index, id, node) =>
      fingersRefs(index) = node
      fingersKeys(index) = id
      //log.info("found_finger_successor on self node " + selfKey + " found for index " + index + " and id: " + id)

    case check_predecessor() =>
      if (predecessorTTL < System.currentTimeMillis()) {
        predecessorRef = null
        predecessorKey = -1
        //log.info("check_predecessor on self node " + selfKey + " timeout")
      }

    case keepAlive() =>
      if (predecessorKey > 0) {
        //log.info("heartBeat on self node " + selfKey)
        predecessorRef ! keepAliveSignal()
      }

    case keepAliveSignal() =>
      //log.info("areYouAlive on self node " + selfKey)
      sender() ! keepAliveReply()

    case keepAliveReply() =>
      predecessorTTL = System.currentTimeMillis() + TTL

    //log.info("yesIAm on self node " + selfKey + " gets new ttl: " + predecessorTTL)

    case refreshMySubscriptions() =>
      subscriptionsAlreadySent.forEach(subscription => self ! find_successor(intSHA1Hash(subscription.topic), self, Some(subscription)))

    case checkMyTopicsSubscribersTTL() =>
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
/*        subscriptionsTTLEntry.keySet().forEach(subscriberId => {
          if (System.currentTimeMillis() > subscriptionsTTLEntry.get(subscriberId) + TTL) {
            subscriptionsTTLEntry.remove(subscriberId)
            topicsWithSubscriptionsEntry.remove(subscriberId)
          }
        })*/
      })

    case NodeFailure =>
      context.stop(self)

    case debug() =>
      log.info("m: " + m + " --- from node " + selfKey)
      log.info("ringSize: " + ringSize + " --- from node " + selfKey)
      log.info("selfKey: " + selfKey + " --- from node " + selfKey)
      log.info("predecessorKey: " + predecessorKey + " --- from node " + selfKey)
      log.info("predecessorTTL: " + predecessorTTL + " --- from node " + selfKey)
      log.info("next: " + next + " --- from node " + selfKey)
      for (i <- 0 until m - 1) {
        log.info("finger " + i + ":" + fingersKeys(i) + " --- from node " + selfKey)
      }
      Thread.sleep(500)
      fingersRefs(0) ! debug()
  }
}
