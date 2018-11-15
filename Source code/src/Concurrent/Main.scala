package concurrent

import akka.actor.{ActorSystem, Props}
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem("SubscribeChordMain")
    actorSystem.actorOf(Props[ChordTester], "ChordTester")
  }
}
