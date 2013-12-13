package sample.cluster.counter

import scala.concurrent.duration._
import akka.actor.Actor
import akka.contrib.pattern.ClusterSharding
import scala.concurrent.forkjoin.ThreadLocalRandom
import akka.actor.ActorLogging

object Bot {
  case object Tick
}

class Bot extends Actor with ActorLogging {
  import Bot._
  import context.dispatcher
  val tickTask = context.system.scheduler.schedule(3.seconds, 3.seconds, self, Tick)

  val region = ClusterSharding(context.system).shardRegion("Counter")
  val anotherRegion = ClusterSharding(context.system).shardRegion("AnotherCounter")

  def rnd = ThreadLocalRandom.current

  override def postStop(): Unit = {
    super.postStop()
    tickTask.cancel()
  }

  def receive = {
    case Tick =>

      val entryId = rnd.nextInt(100)
      val operation = rnd.nextInt(5) match {
        case 0 => Counter.Get(entryId)
        case 1 => Counter.EntryEnvelope(entryId, Counter.Decrement)
        case _ => Counter.EntryEnvelope(entryId, Counter.Increment)
      }

      val r = if (rnd.nextBoolean()) region else anotherRegion
      log.info("Sending operation {} to {} with entryId {}", operation,
        r.path.name, entryId)
      r ! operation

    case value: Int =>
      log.info("Current value at {} is [{}]", sender.path, value)
  }

}