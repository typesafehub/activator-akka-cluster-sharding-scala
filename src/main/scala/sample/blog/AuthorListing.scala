package sample.blog

import scala.collection.immutable
import scala.concurrent.duration._
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.PoisonPill
import akka.actor.ReceiveTimeout
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.Passivate
import akka.persistence.PersistentActor
import akka.persistence.SnapshotOffer
import akka.persistence.RecoveryCompleted

object AuthorListing {

  def props(): Props = Props(new AuthorListing)

  case class PostSummary(author: String, postId: String, title: String)
  case class GetPosts(author: String)
  case class Posts(list: immutable.IndexedSeq[PostSummary])

  val idExtractor: ShardRegion.ExtractEntityId = {
    case s: PostSummary => (s.author, s)
    case m: GetPosts    => (m.author, m)
  }

  val shardResolver: ShardRegion.ExtractShardId = msg => msg match {
    case s: PostSummary   => (math.abs(s.author.hashCode) % 30).toString
    case GetPosts(author) => (math.abs(author.hashCode) % 30).toString
  }

  val shardName: String = "AuthorListing"
}

class AuthorListing extends PersistentActor with ActorLogging {
  import AuthorListing._

  override def persistenceId: String = self.path.parent.name + "-" + self.path.name

  val startTime = System.nanoTime()
  log.debug("new instance: {}", self.path.name)

  // passivate the entity when no activity
  context.setReceiveTimeout(2.minutes)

  var posts = Vector.empty[PostSummary]

  def receiveCommand = {
    case s: PostSummary =>
      persist(s) { evt =>
        handleEvent(evt)
        log.debug("Post added to {}'s list: {}", s.author, s.title)
        if (lastSequenceNr % 1000 == 0) {
          log.info("Save snapshot at sequenceNr {}", lastSequenceNr)
          saveSnapshot(posts)
        }
      }
    case GetPosts(_) =>
      sender() ! Posts(posts)
    case ReceiveTimeout => context.parent ! Passivate(stopMessage = PoisonPill)
  }

  override def receiveRecover: Receive = {
    case evt: PostSummary                            => handleEvent(evt)
    case SnapshotOffer(_, snap: Vector[PostSummary]) => posts = snap
    case RecoveryCompleted =>
      val duration = (System.nanoTime() - startTime) / 1000 / 1000
      log.info("recovery completed in {} ms : {}", duration, self.path.name)
  }

  private def handleEvent(evt: PostSummary): Unit = {
    posts = (posts :+ evt).takeRight(10)
  }

}
