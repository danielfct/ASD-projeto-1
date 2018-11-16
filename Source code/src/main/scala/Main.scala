
import akka.actor.{ActorSystem, Props}
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    /*if (args.length != 3) {
      println("Usage: sbt run NumberOfNodes NumberOfRequests NodeFailurePercentage")
      System.exit(1)
    }
    val maxNrNodes: Int = math.min(8, args(0).toInt)
    val numberOfRequests: Int = args(1).toInt
    val nodeFailurePercentage: Float = math.min(0.9f, args(1).toFloat)
    */
    val maxNrNodes: Int = 128
    val numberOfRequests: Int = 10
    val nodeFailurePercentage: Float = 0.2f

    val actorSystem = ActorSystem("PublishSubscribeChord")

    actorSystem.actorOf(Props(new ChordTester(maxNrNodes, numberOfRequests, nodeFailurePercentage)), "PublishSubscribeChordTester")
  }
}
