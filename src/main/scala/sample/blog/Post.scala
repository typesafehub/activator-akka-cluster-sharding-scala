package sample.blog

import scala.concurrent.duration._
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.contrib.pattern.ShardRegion
import akka.contrib.pattern.ShardRegion.Passivate
import akka.persistence.EventsourcedProcessor
import akka.persistence.Persistent

object Post {

  def props(authorListing: ActorRef): Props =
    Props(new Post(authorListing))

  object PostContent {
    val empty = PostContent("", "", "")
  }
  case class PostContent(author: String, title: String, body: String)

  sealed trait Command {
    def postId: String
  }
  case class AddPost(postId: String, content: PostContent) extends Command
  case class GetContent(postId: String) extends Command
  case class ChangeBody(postId: String, body: String) extends Command
  case class Publish(postId: String) extends Command

  sealed trait Event
  case class PostAdded(content: PostContent) extends Event
  case class BodyChanged(body: String) extends Event
  case object PostPublished extends Event

  val idExtractor: ShardRegion.IdExtractor = {
    case cmd: Command => (cmd.postId, cmd)
  }

  val shardResolver: ShardRegion.ShardResolver = msg => msg match {
    case cmd: Command => (math.abs(cmd.postId.hashCode) % 100).toString
  }

  val shardName: String = "Post"

  private case class State(content: PostContent, published: Boolean) {

    def updated(evt: Event): State = evt match {
      case PostAdded(c)   => copy(content = c)
      case BodyChanged(b) => copy(content = content.copy(body = b))
      case PostPublished  => copy(published = true)
    }
  }
}

class Post(authorListing: ActorRef) extends EventsourcedProcessor with ActorLogging {

  import Post._

  // passivate the entity when no activity
  context.setReceiveTimeout(2.minutes)

  private var state = State(PostContent.empty, false)

  override def receiveRecover: Receive = {
    case evt: Event => state = state.updated(evt)
  }

  override def receiveCommand: Receive = {
    case cmd: Command => cmd match {
      case GetContent(_) => sender() ! state.content
      case AddPost(_, content) =>
        if (state.content == PostContent.empty && content.author != "" && content.title != "")
          persist(PostAdded(content)) { evt =>
            state = state.updated(evt)
            log.info("New post saved: {}", state.content.title)
          }
      case ChangeBody(_, body) =>
        if (!state.published)
          persist(BodyChanged(body)) { evt =>
            state = state.updated(evt)
            log.info("Post changed: {}", state.content.title)
          }
      case Publish(postId) =>
        if (!state.published)
          persist(PostPublished) { evt =>
            state = state.updated(evt)
            val c = state.content
            log.info("Post published: {}", c.title)
            authorListing ! Persistent(AuthorListing.PostSummary(c.author, postId, c.title))
          }
    }
    case ReceiveTimeout => context.parent ! Passivate(stopMessage = PoisonPill)
  }

}
