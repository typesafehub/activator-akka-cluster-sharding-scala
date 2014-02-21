package sample.blog

import java.util.UUID
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.cluster.Cluster
import akka.contrib.pattern.ClusterSharding

object Bot {
  private case object Tick
}

class Bot extends Actor with ActorLogging {
  import Bot._
  import context.dispatcher
  val tickTask = context.system.scheduler.schedule(3.seconds, 3.seconds, self, Tick)

  val postRegion = ClusterSharding(context.system).shardRegion(Post.shardName)
  val listingsRegion = ClusterSharding(context.system).shardRegion(AuthorListing.shardName)

  val from = Cluster(context.system).selfAddress.hostPort

  override def postStop(): Unit = {
    super.postStop()
    tickTask.cancel()
  }

  var n = 0
  val authors = Map(0 -> "Patrik", 1 -> "Martin", 2 -> "Roland", 3 -> "Björn", 4 -> "Endre")
  def currentAuthor = authors(n % authors.size)

  def receive = create

  val create: Receive = {
    case Tick =>
      val postId = UUID.randomUUID().toString
      n += 1
      val title = s"Post $n from $from"
      postRegion ! Post.AddPost(postId, Post.PostContent(currentAuthor, title, "..."))
      context.become(edit(postId))
  }

  def edit(postId: String): Receive = {
    case Tick =>
      postRegion ! Post.ChangeBody(postId, "Something very interesting ...")
      context.become(publish(postId))
  }

  def publish(postId: String): Receive = {
    case Tick =>
      postRegion ! Post.Publish(postId)
      context.become(list)
  }

  val list: Receive = {
    case Tick =>
      listingsRegion ! AuthorListing.GetPosts(currentAuthor)
    case AuthorListing.Posts(summaries) =>
      log.info("Posts by {}: {}", currentAuthor, summaries.map(_.title).mkString("\n\t", "\n\t", ""))
      context.become(create)
  }

}
