package Concurrent

import akka.actor.{ActorRef, ActorSystem}

case class find_successor(id: Int, node: ActorRef)

case class found_successor(id: Int, node: ActorRef)

//case class create(sys: ActorSystem, m: Int, node: ActorRef)

case class create(sys: ActorSystem, m: Int, id: Int, node: ActorRef)

case class initJoin(node: ActorRef)

case class join(id: Int, node: ActorRef)

case class stabilize()

case class stabilizeAskSuccessorPredecessor()

case class stabilizeSendSuccessorPredecessor(id: Int, node: ActorRef)

case class stabilizeReceiveSuccessorPredecessor(id: Int, node: ActorRef)

case class notification(id: Int, node: ActorRef)

case class fix_fingers()

case class find_finger_successor(index: Int, id: Int, node: ActorRef)

case class found_finger_successor(index: Int, id: Int, node: ActorRef)

case class check_predecessor()

case class heartBeat()

case class areYouAlive()

case class yesIAm()

case class debug()