package com.example

import akka.actor.{ActorSystem, Props}
import akka.actor.actorRef2Scala

//import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {

    val actorSystem = ActorSystem("SubscribeChord")
    //import actorSystem.dispatcher

    val factor: Int = 7

    val actor1 = actorSystem.actorOf(Props[SubscribeChord], "Guy1")
    val actor2 = actorSystem.actorOf(Props[SubscribeChord], "Guy2")
    val actor3 = actorSystem.actorOf(Props[SubscribeChord], "Guy3")

    actor1 ! create(factor, actor1)
    actor2 ! create(factor, actor1)
    actor3 ! create(factor, actor1)
  }
}
