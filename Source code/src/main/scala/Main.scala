
import akka.actor.{ActorSystem, Props}
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    /*if (args.length != 2) {
      println("Usage: sbt run NumberOfNodes NumberOfRequests")
      System.exit(1)
    }
    val maxNrNodes = math.min(8, args(0).toInt))
    val numberOfRequests: Int = args(1).toInt
    */
    val maxNrNodes: Int = 128
    val nrRequests: Int = 10

    val actorSystem = ActorSystem("PublishSubscribeChord")

    actorSystem.actorOf(Props(new ChordTester(maxNrNodes, nrRequests)), "PublishSubscribeChordTester")
  }
}
