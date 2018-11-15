package concurrent

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
//import scala.util.Random
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

  //val r = new Random

  var m: Int = 0

  var ringSize: Int = 0

  var selfRef: ActorRef = null

  var selfKey: Int = 0

  var predecessorRef: ActorRef = null

  var predecessorKey: Int = 0

  var predecessorTTL: Long = 0

  var fingersRefs = new Array[ActorRef](1)

  var fingersKeys = new Array[Int](1)

  var next: Int = 0
  
  var topicsWithSubscriptions = new HashMap[String, Map[Integer, ActorRef]]
  
  var subscriptionsAlreadySent = new LinkedList[Message]
  
  var subscriptionsTTL = new HashMap[String, Map[Integer, Long]]
  
  val TTL = 15000 // TODO: find a good duration

  def isInInterval(value: Int, start: Int, end: Int, includeStart: Boolean, includeEnd: Boolean): Boolean = {
    //log.info("{}: isInInterval value={}, start={}, end={}", selfKey, value, start, end);
    //log.info("{}: isInInterval includeStart={}, includeEnd={}", selfKey, includeStart, includeEnd);
    var res = false;

    if (start == end && value != start && includeStart != includeEnd) {
      //log.info("result true={}", res)
      return true
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

    //log.info("res= {}", res)  
    return res
  }

  def closest_preceding_finger(id: Int): ActorRef = {
    for (i <- (m - 1) to 0 by -1) {
      //log.info("index: {}", i)
      if (isInInterval(fingersKeys(i), selfKey, id, false, false)) {
        //log.info("closest_preceding_finger: " + i)
        return fingersRefs(i)
      }
    }

    //log.info("closest_preceding_finger: " + -1)
    selfRef
  }
  
  def intSHA1Hash(topic: String): Int = {
    val sha1Bytes = MessageDigest.getInstance("SHA-1").digest(topic.getBytes)
    return ByteBuffer.wrap(sha1Bytes).getInt % ringSize
  }

  override def receive = {
    
    case sendMessage(topic: String, msgType: String, msg: String) =>
      val m = new Message(topic, msgType, msg, selfKey, selfRef)
      if (msgType == "SUBSCRIBE")
        subscriptionsAlreadySent.add(m)
      selfRef ! route(intSHA1Hash(topic), Some(m))
    
    case route (id, message) =>
      if (id <= selfKey) {
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
              
              topicSubscribers.put(message.originalId, message.originalRef)
              topicSubscribersTTL.put(message.originalId, System.currentTimeMillis())
            case "UNSUBSCRIBE" =>
              var topicSubscribers = topicsWithSubscriptions.get(message.topic)
              var topicSubscribersTTL = subscriptionsTTL.get(message.topic)
              if (topicSubscribers != null) {
                topicSubscribers.remove(message.originalId)
                topicSubscribersTTL.remove(message.originalId)
              }
            case "PUBLISH" =>
              var subscribers = topicsWithSubscriptions.get(message.topic).values()
              subscribers.forEach(sub => sub ! messageDelivery(message.msg))
          }
          case None => log.info("Warning: route got an empty message!");
        }
      } else selfRef ! find_successor(id, selfRef, message)
      
      
    case messageDelivery(message) =>
      log.info("{} got message = {}", selfKey, message)
      
    case find_successor(id, node, message) =>
      if (isInInterval(id, selfKey, fingersKeys(0), false, true)) {
        //log.info("find_successor on self node " + selfKey + " with id " + id + " result: " + fingersKeys(0))

        message match {
          case None => node ! found_successor(fingersKeys(0), fingersRefs(0))
          case message => fingersRefs(0) ! route(id, message)
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

    //case create(sys, factor, contactNode) =>
    case create(factor, id, contactNode) =>
      m = factor

      ringSize = Math.pow(2, m.toDouble).toInt

      selfRef = self

      //selfKey = r.nextInt(ringSize)

      selfKey = id

      fingersRefs = new Array[ActorRef](m)

      fingersKeys = new Array[Int](m)

      predecessorRef = null

      predecessorKey = -1

      for (i <- 0 until m - 1) {
        fingersRefs(i) = selfRef
        fingersKeys(i) = selfKey
      }

      context.system.scheduler.schedule(0 milliseconds, 5000 milliseconds, selfRef, stabilize())
      context.system.scheduler.schedule(0 milliseconds, 5000 milliseconds, selfRef, fix_fingers())
      context.system.scheduler.schedule(0 milliseconds, 10000 milliseconds, selfRef, check_predecessor())
      context.system.scheduler.schedule(0 milliseconds, 5000 milliseconds, selfRef, heartBeat())
      
      context.system.scheduler.schedule(0 milliseconds, 5 seconds, selfRef, refreshMySubscriptions()) // TODO: find a good duration
      context.system.scheduler.schedule(0 milliseconds, 15 seconds, selfRef, checkMyTopicsSubscribersTTL()) // TODO: find a good duration

      //log.info("create self node: " + selfKey)

      if (contactNode != null && contactNode != selfRef)
        selfRef ! initJoin(contactNode)

    case initJoin(contactNode) =>
      predecessorRef = null

      predecessorKey = -1

      //log.info("initJoin on self node " + selfKey)

      contactNode ! join(selfKey, selfRef)

    case join(id, node) =>

      //log.info("join on self node " + selfKey + " with node id " + id)

      selfRef ! find_successor(id, node, None)

    case stabilize() =>
      //log.info("stabilize on self node " + selfKey)

      fingersRefs(0) ! stabilizeAskSuccessorPredecessor()

    case stabilizeAskSuccessorPredecessor() =>
      //log.info("stabilizeAskSuccessorPredecessor on self node " + selfKey)

      sender() ! stabilizeSendSuccessorPredecessor(predecessorKey, predecessorRef)

    case stabilizeSendSuccessorPredecessor(id, node) =>
      if (id != -1 && isInInterval(id, selfKey, fingersKeys(0), false, false)) {
        fingersRefs(0) = node
        fingersKeys(0) = id
        //log.info("stabilizeSendSuccessorPredecessor on self node " + selfKey + " finds successor: " + id)
      }

      if (fingersRefs(0) != selfRef) {
        //log.info("start notification on self node " + selfKey)

        fingersRefs(0) ! notification(selfKey, selfRef)
      }

    case notification(id, node) =>
      if (predecessorKey == -1 || isInInterval(id, predecessorKey, selfKey, false, false)) {
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

      selfRef ! find_finger_successor(next, selfKey + Math.pow(2, next - 1).toInt, selfRef)

    case find_finger_successor(index, id, node) =>
      if (isInInterval(id, selfKey, fingersKeys(0), false, true)) {
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

    case heartBeat() =>
      if (predecessorKey > 0) {
        //log.info("heartBeat on self node " + selfKey)

        predecessorRef ! areYouAlive()
      }

    case areYouAlive() =>
      //log.info("areYouAlive on self node " + selfKey)
      sender() ! yesIAm()

    case yesIAm() =>
      predecessorTTL = System.currentTimeMillis() + 15000

    //log.info("yesIAm on self node " + selfKey + " gets new ttl: " + predecessorTTL)
      
    case refreshMySubscriptions() =>
      subscriptionsAlreadySent.forEach(subscription => selfRef ! route(intSHA1Hash(subscription.topic), Some(subscription)))
      
    case checkMyTopicsSubscribersTTL() =>
      subscriptionsTTL.keySet().forEach(topic => {
        val subscriptionsTTLEntry = subscriptionsTTL.get(topic)
        val topicsWithSubscriptionsEntry = topicsWithSubscriptions.get(topic)
        
        subscriptionsTTLEntry.keySet().forEach(subscriberId => {
          if (subscriptionsTTLEntry.get(subscriberId) + TTL < System.currentTimeMillis()) {
            subscriptionsTTLEntry.remove(subscriberId)
            topicsWithSubscriptionsEntry.remove(subscriberId)
          }
        })
      })
      
    case PoisonPill =>
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
