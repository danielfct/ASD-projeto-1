package concurrent

import akka.actor.{ActorSystem, Props}
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {

    val r = new Random

    val actorSystem = ActorSystem("SubscribeChord")
    import actorSystem.dispatcher

    val factor: Int = 7

    val maxNumNodes = Math.pow(2, factor.toDouble).toInt

    val ids: Array[Int] = new Array[Int](maxNumNodes)

    val actorInit = actorSystem.actorOf(Props[SubscribeChord], "Initializer")

    var identification: Int = r.nextInt(maxNumNodes)

    ids(identification) = 1

    actorInit ! create(actorSystem, factor, identification, actorInit)

    for (i <- 0 until 100) {
      identification = r.nextInt(maxNumNodes)

      while (ids(identification) == 1) {
        identification = r.nextInt(maxNumNodes)
      }

      ids(identification) = 1

      actorSystem.actorOf(Props[SubscribeChord], "Node" + identification) ! create(actorSystem, factor, identification, actorInit)
    }
  }
}
