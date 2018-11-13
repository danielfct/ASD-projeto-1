package com.example

import akka.actor.{Actor, ActorLogging, ActorRef}
import scala.util.Random
class SubscribeChord extends Actor with ActorLogging {

  //var system: ActorSystem = null

  val r = new Random

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

  val stab = new Thread {
    override def run: Unit = {
      while (true) {
        Thread.sleep(5000)
        selfRef ! stabilize()
      }
    }
  }
  stab.start

  val checkPre = new Thread {
    override def run: Unit = {
      while (true) {
        Thread.sleep(15000)
        selfRef ! check_predecessor()
      }
    }
  }
  checkPre.start

  val beat = new Thread {
    override def run: Unit = {
      while (true) {
        Thread.sleep(5000)
        if (predecessorKey > 0) {
          predecessorRef ! heartBeat()
        }
      }
    }
  }
  beat.start
  
  def isInInterval(value: Int, start: Int, end: Int, includeStart: Boolean, includeEnd: Boolean) : Boolean = {
    //log.info("{}: isInInterval value={}, start={}, end={}", selfKey, value, start, end);
    //log.info("{}: isInInterval includeStart={}, includeEnd={}", selfKey, includeStart, includeEnd);
    var intervalStart = if(includeStart) start else start + 1
    var intervalEnd = if(includeEnd) end else end - 1
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
      log.info("index: {}", i)
      if (isInInterval(fingersKeys(i), selfKey, id, false, false)) {
        log.info("closest_preceding_finger: " + i)
        return fingersRefs(i)
      }
    }

    log.info("closest_preceding_finger: " + -1)
    selfRef
  }

  override def receive = {
    case find_successor(id, node) =>
      if (isInInterval(id, selfKey, fingersKeys(0), false, true)) {
        log.info("find_successor: " + fingersKeys(0))
        node ! found_successor(fingersKeys(0), fingersRefs(0))
      }

      else {
        log.info("find_successor: continue")

        closest_preceding_finger(id) ! find_successor(id, node)
      }

    case found_successor(id, node) =>
      fingersRefs(0) = node
      fingersKeys(0) = id
      log.info("{}: ok, my successor is {}", selfKey, id)

    case create(factor, contactNode) =>
      m = factor

      ringSize = Math.pow(2, m.toDouble).toInt

      selfRef = self

      selfKey = r.nextInt(ringSize)
      
      fingersRefs = new Array[ActorRef](m)

      fingersKeys = new Array[Int](m)

      predecessorRef = null

      predecessorKey = -1

     for (i <- 0 until m - 1) {
        fingersRefs(i) = selfRef
        fingersKeys(i) = selfKey
      }

      log.info("Create: " + selfKey)
      
     if (contactNode != null && contactNode != selfRef)
        selfRef ! initJoin(contactNode)

    case initJoin(contactNode) =>
      predecessorRef = null

      predecessorKey = -1

      log.info("Init join: " + selfKey)

      contactNode ! join(selfKey, selfRef)

    case join(id, node) =>

      log.info("Join: " + selfKey)

      selfRef ! find_successor(id, node)

    case stabilize() =>
      fingersRefs(0) ! stabilizeAskSuccessorPredecessor()

    case stabilizeAskSuccessorPredecessor() =>
      sender() ! stabilizeSendSuccessorPredecessor(predecessorKey, predecessorRef)

    case stabilizeSendSuccessorPredecessor(id, node) =>
      if (id != -1 && isInInterval(id, selfKey, fingersKeys(0), false, false)) {
        fingersRefs(0) = node
        fingersKeys(0) = id
        log.info("{} stabilize ok, my successor is {}", selfKey, id)
      }

      if (fingersRefs(0) != selfRef)
        fingersRefs(0) ! notification(selfKey, selfRef)

    case notification(id, node) =>
      if (predecessorKey == -1 || isInInterval(id, predecessorKey, selfKey, false, false)) {
        predecessorRef = node
        predecessorKey = id
        log.info("{} notification ok, my predecessor is {}", selfKey, id)

      }

    case fix_fingers() =>
      next = next + 1

      if (next > (m-1)) {
        next = 0
      }

      selfRef ! find_finger_successor(next, selfKey + Math.pow(2, next - 1).toInt, selfRef)

    case find_finger_successor(index, id, node) =>
      if (isInInterval(id,selfKey,fingersKeys(0),false,true)) {
        node ! found_finger_successor(index, fingersKeys(0), fingersRefs(0))
      }

      else {
        closest_preceding_finger(id) ! find_finger_successor(index, id, node)
      }

    case found_finger_successor(index, id, node) =>
      fingersRefs(index) = node

      fingersKeys(index) = id

    case check_predecessor() =>
      if (predecessorTTL < System.currentTimeMillis()) {
        predecessorRef = null

        predecessorKey = -1
      }

    case heartBeat() =>
      if (predecessorKey > 0) {
        predecessorRef ! areYouAlive()
      }
      
    case areYouAlive() =>
      sender() ! yesIAm()

    case yesIAm() =>
      predecessorTTL = System.currentTimeMillis() + 15000

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
