package sample.cluster.counter

import scala.concurrent.duration._
import akka.actor.ReceiveTimeout
import akka.contrib.pattern.ShardRegion
import akka.persistence.EventsourcedProcessor

object Counter {
  case object Increment
  case object Decrement
  case class Get(counterId: Long)
  case class EntryEnvelope(id: Long, payload: Any)

  case object Stop
  case class CounterChanged(delta: Int)

  val idExtractor: ShardRegion.IdExtractor = {
    case EntryEnvelope(id, payload) => (id.toString, payload)
    case msg @ Get(id)              => (id.toString, msg)
  }

  val shardResolver: ShardRegion.ShardResolver = msg => msg match {
    case EntryEnvelope(id, _) => (id % 30).toString
    case Get(id)              => (id % 30).toString
  }
}

class Counter extends EventsourcedProcessor {
  import Counter._
  import ShardRegion.Passivate

  context.setReceiveTimeout(120.seconds)

  var count = 0

  def updateState(event: CounterChanged): Unit =
    count += event.delta

  override def receiveReplay: Receive = {
    case evt: CounterChanged => updateState(evt)
  }

  override def receiveCommand: Receive = {
    case Increment      => persist(CounterChanged(+1))(updateState)
    case Decrement      => persist(CounterChanged(-1))(updateState)
    case Get(_)         => sender ! count
    case ReceiveTimeout => context.parent ! Passivate(stopMessage = Stop)
    case Stop           => context.stop(self)
  }
}