package sample.blog

import scala.collection.immutable
import scala.concurrent.duration._
import akka.actor.ActorLogging
import akka.actor.PoisonPill
import akka.actor.ReceiveTimeout
import akka.contrib.pattern.ShardRegion
import akka.contrib.pattern.ShardRegion.Passivate
import akka.persistence.Persistent
import akka.persistence.Processor

object AuthorListing {
  case class PostSummary(author: String, postId: String, title: String)
  case class GetPosts(author: String)
  case class Posts(list: immutable.IndexedSeq[PostSummary])

  val idExtractor: ShardRegion.IdExtractor = {
    case p @ Persistent(s: PostSummary, _) => (s.author, p)
    case m: GetPosts                       => (m.author, m)
  }

  val shardResolver: ShardRegion.ShardResolver = msg => msg match {
    case Persistent(s: PostSummary, _) => (math.abs(s.author.hashCode) % 100).toString
    case GetPosts(author)              => (math.abs(author.hashCode) % 100).toString
  }

  val shardName: String = "AuthorListing"
}

class AuthorListing extends Processor with ActorLogging {
  import AuthorListing._

  // passivate the entity when no activity
  context.setReceiveTimeout(2.minutes)

  var posts = Vector.empty[PostSummary]

  def receive = {
    case Persistent(s: PostSummary, _) =>
      posts :+= s
      log.info("Post added to {}'s list: {}", s.author, s.title)
    case GetPosts(_) =>
      sender() ! Posts(posts)
    case ReceiveTimeout => context.parent ! Passivate(stopMessage = PoisonPill)
  }

}