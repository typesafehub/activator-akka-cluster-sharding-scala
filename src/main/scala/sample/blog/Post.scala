package sample.blog

import scala.concurrent.duration._
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.contrib.pattern.ShardRegion
import akka.contrib.pattern.ShardRegion.Passivate
import akka.persistence.PersistentActor

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

class Post(authorListing: ActorRef) extends PersistentActor with ActorLogging {

  import Post._

  // self.path.parent.name is the type name (utf-8 URL-encoded) 
  // self.path.name is the entry identifier (utf-8 URL-encoded)
  override def persistenceId: String = self.path.parent.name + "-" + self.path.name

  // passivate the entity when no activity
  context.setReceiveTimeout(2.minutes)

  private var state = State(PostContent.empty, false)

  override def receiveRecover: Receive = {
    case evt: PostAdded =>
      context.become(created)
      state = state.updated(evt)
    case evt @ PostPublished =>
      context.become(created)
      state = state.updated(evt)
    case evt: Event => state =
      state.updated(evt)
  }

  override def receiveCommand: Receive = initial

  def initial: Receive = {
    case GetContent(_) => sender() ! state.content
    case AddPost(_, content) =>
      if (content.author != "" && content.title != "")
        persist(PostAdded(content)) { evt =>
          state = state.updated(evt)
          context.become(created)
          log.info("New post saved: {}", state.content.title)
        }
  }

  def created: Receive = {
    case GetContent(_) => sender() ! state.content
    case ChangeBody(_, body) =>
      persist(BodyChanged(body)) { evt =>
        state = state.updated(evt)
        log.info("Post changed: {}", state.content.title)
      }
    case Publish(postId) =>
      persist(PostPublished) { evt =>
        state = state.updated(evt)
        context.become(published)
        val c = state.content
        log.info("Post published: {}", c.title)
        authorListing ! AuthorListing.PostSummary(c.author, postId, c.title)
      }
  }

  def published: Receive = {
    case GetContent(_) => sender() ! state.content
  }

  override def unhandled(msg: Any): Unit = msg match {
    case ReceiveTimeout => context.parent ! Passivate(stopMessage = PoisonPill)
    case _              => super.unhandled(msg)
  }

}
