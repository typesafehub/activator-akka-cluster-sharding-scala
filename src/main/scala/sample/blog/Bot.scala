package sample.blog

import java.util.UUID
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.cluster.Cluster
import akka.cluster.sharding.ClusterSharding
import akka.actor.ReceiveTimeout

object Bot {
  private case object Tick
}

class Bot extends Actor with ActorLogging {
  import Bot._
  import context.dispatcher
  //  val tickTask = context.system.scheduler.schedule(3.seconds, 3.seconds, self, Tick)
  self ! Tick
  context.setReceiveTimeout(15.seconds)

  val postRegion = ClusterSharding(context.system).shardRegion(Post.shardName)
  val listingsRegion = ClusterSharding(context.system).shardRegion(AuthorListing.shardName)

  val from = Cluster(context.system).selfAddress.hostPort

  override def postStop(): Unit = {
    super.postStop()
    //    tickTask.cancel()
  }

  val N = 100000
  var n = 0
  val authors = Map(0 -> "Patrik", 1 -> "Martin", 2 -> "Roland", 3 -> "Björn", 4 -> "Endre")
  def currentAuthor = authors(n % authors.size)

  def receive = create

  val create: Receive = {
    case Tick =>

      //      val postId = UUID.randomUUID().toString
      n += 1
      val postId = Cluster(context.system).selfAddress.port.get + "-" + n
      val title = s"Post $n from $from"
      postRegion ! Post.AddPost(postId, Post.PostContent(currentAuthor, title, "..."))
      context.become(edit(postId))
      if (n < N) {
        if (n % 1000 == 0)
          log.info(s"Created $n posts")
        self ! Tick
      } else if (n == N) {
        log.info(s"Created ALL $n posts")
        context.system.scheduler.schedule(3.seconds, 3.seconds, self, Tick)
      }
  }

  def edit(postId: String): Receive = {
    case Tick =>
      postRegion ! Post.ChangeBody(postId, "Something very interesting ...")
      context.become(publish(postId))
      if (n < N) self ! Tick
  }

  def publish(postId: String): Receive = {
    case Tick =>
      postRegion ! Post.Publish(postId)
    case Post.PostPublished =>
      context.become(list)
      if (n < N) self ! Tick
  }

  val list: Receive = {
    case Tick =>
      listingsRegion ! AuthorListing.GetPosts(currentAuthor)
    case AuthorListing.Posts(summaries) =>
      context.become(create)
      if (n < N)
        self ! Tick
      else
        log.info("Latest posts by {}: {}", currentAuthor, summaries.map(_.title).mkString("\n\t", "\n\t", ""))
  }

  override def unhandled(msg: Any): Unit = msg match {
    case ReceiveTimeout =>
      log.info("ReceiveTimeout {} ", n)
      context.become(create)
      if (n < N) self ! Tick
  }

}
