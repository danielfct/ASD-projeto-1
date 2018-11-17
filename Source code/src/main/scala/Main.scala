
import akka.actor.{ActorSystem, Props}
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    /*if (args.length != 3) {
      println("Usage: sbt run NumberOfNodes NumberOfRequests NodeFailurePercentage")
      System.exit(1)
    }
    val maxNrNodes: Int = args(0).toInt
    val numberOfRequests: Int = math.max(10, args(1).toInt)
    val nodeFailurePercentage: Float = math.min(args(1).toFloat, 0.9f)
    */
    val maxNrNodes: Int = 5
    val numberOfRequests: Int = 10
    val nodeFailurePercentage: Float = 0f

    val actorSystem = ActorSystem("PublishSubscribeChord")

    actorSystem.actorOf(Props(new ChordTester(maxNrNodes, numberOfRequests, nodeFailurePercentage)), "PublishSubscribeChordTester")
  }
}
