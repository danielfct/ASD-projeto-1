
import akka.actor.{ActorRef, ActorSystem}
import java.util.Map

case class find_successor(id: Int, node: ActorRef, message: Option[Message])

case class found_successor(id: Int, node: ActorRef)

//case class create(sys: ActorSystem, m: Int, node: ActorRef)

case class create(m: Int, id: Int, node: ActorRef)

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

case class keepAlive()

case class keepAliveSignal()

case class keepAliveReply()

case class debug()

case class sendMessage(topic: String, msgType: String, msg: String)

case class route(id: Int, message: Option[Message])

case class messageDelivery(message: String)

case class refreshMySubscriptions()

case class checkMyTopicsSubscribersTTL()

class Message(var topic: String, var msgType: String, var msg: String, var originalId: Int, var originalRef: ActorRef)

final case object CountMessage

final case object NodeFailure