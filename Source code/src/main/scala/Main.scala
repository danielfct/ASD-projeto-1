import ChordTester.{CreateRing, Publish, ShowStats, Subscribe}
import akka.actor.{ActorSystem, Props}

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
    val maxNrNodes: Int = 100
    val numberOfRequests: Int = 10
    val nodeFailurePercentage: Float = 0f //TODO para suportar falhas é preciso modificar o chord (successorList)

    val actorSystem = ActorSystem("PublishSubscribeChord")
    val tester = actorSystem.actorOf(Props(new ChordTester(maxNrNodes, numberOfRequests, nodeFailurePercentage)), "PublishSubscribeChordTester")
    tester ! CreateRing
    Thread.sleep(15000)
    tester ! Subscribe
    Thread.sleep(2000)
    for (_ <- 0 until numberOfRequests) {
      tester ! Publish
      Thread.sleep(1000)
    }
    Thread.sleep(2500)
    tester ! ShowStats
    Thread.sleep(2500)
    actorSystem.terminate()
    System.exit(0)
  }

}
