package concurrent

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
//import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.HashMap
import java.util.Map

class SubscribeChord extends Actor with ActorLogging {

  var system: ActorSystem = null

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

  override def receive = {
    case route (id, message) =>
      if (id <= selfKey) {
        message match {
          case Some(message) => message.msgType match {
            case "SUBSCRIBE" => 
              var topicSubscribers = topicsWithSubscriptions.get(message.topic)
              if (topicSubscribers == null) {
                topicsWithSubscriptions.put(message.topic, new HashMap[Integer, ActorRef]())
                topicSubscribers = topicsWithSubscriptions.get(message.topic)
              }
              
              topicSubscribers.put(message.originalId, message.originalRef)  
            case "UNSUBSCRIBE" =>
              var topicSubscribers = topicsWithSubscriptions.get(message.topic)
              if (topicSubscribers != null)
                topicSubscribers.remove(message.originalId)
            case "PUBLISH" =>
              var subscribers = topicsWithSubscriptions.get(message.topic).values()
              subscribers.forEach(sub => sub ! messageDevivery(message.msg))
          }
          case None => log.info("Warning: route got an empty message!");
        }
      } else selfRef ! find_successor(id, selfRef, message)
      
      
    case messageDevivery(message) =>
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
    case create(sys, factor, id, contactNode) =>
      system = sys

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
