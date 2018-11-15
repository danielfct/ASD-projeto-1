package concurrent

import akka.actor.{ActorSystem, Props}
import scala.util.Random
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Terminated

class ChordTester extends Actor with ActorLogging {
  
    val r = new Random

    val actorSystem = ActorSystem("SubscribeChord")
    import actorSystem.dispatcher

    val factor: Int = 7

    val numRequests = 10
    val maxNumNodes = Math.pow(2, factor.toDouble).toInt
    var numMessages = 0
    var numFailedNodes = 0

    val ids: Array[Int] = new Array[Int](maxNumNodes)

    val actorInit = actorSystem.actorOf(SubscribeChord.props(self), "Initializer")
    context.watch(actorInit)

    var identification: Int = r.nextInt(maxNumNodes)

    ids(identification) = 1

    actorInit ! create(factor, identification, actorInit)

    for (i <- 0 until 100) {
      identification = r.nextInt(maxNumNodes)

      while (ids(identification) == 1) {
        identification = r.nextInt(maxNumNodes)
      }

      ids(identification) = 1

      val chordNode = actorSystem.actorOf(SubscribeChord.props(self), "Node" + identification)
      context.watch(chordNode)
      
      chordNode ! create(factor, identification, actorInit)
    }
    
    /*for (i<-1 until numRequests) {
      actorInit ! sendMessage("teste", "SUBSCRIBE", "")
      actorInit ! sendMessage("teste", "PUBLISH", "Hello world")
      actorInit ! sendMessage("teste", "UNSUBSCRIBE", "")
    }*/
    
    override def receive = {
      case Terminated(_) => numFailedNodes += 1
      case CountMessage => numMessages += 1
    }
}