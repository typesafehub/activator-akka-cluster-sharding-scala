package sample.blog

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.ActorIdentity
import akka.actor.ActorPath
import akka.actor.ActorSystem
import akka.actor.Identify
import akka.actor.Props
import akka.contrib.pattern.ClusterSharding
import akka.pattern.ask
import akka.persistence.journal.leveldb.SharedLeveldbJournal
import akka.persistence.journal.leveldb.SharedLeveldbStore
import akka.util.Timeout

object BlogApp {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty)
      startup(Seq("2551", "2552", "0"))
    else
      startup(args)
  }

  def startup(ports: Seq[String]): Unit = {
    ports foreach { port =>
      // Override the configuration of the port
      val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port).
        withFallback(ConfigFactory.load())

      // Create an Akka system
      val system = ActorSystem("ClusterSystem", config)

      startupSharedJournal(system, startStore = (port == "2551"), path =
        ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:2551/user/store"))

      val authorListingRegion = ClusterSharding(system).start(
        typeName = AuthorListing.shardName,
        entryProps = Some(AuthorListing.props()),
        idExtractor = AuthorListing.idExtractor,
        shardResolver = AuthorListing.shardResolver)
      ClusterSharding(system).start(
        typeName = Post.shardName,
        entryProps = Some(Post.props(authorListingRegion)),
        idExtractor = Post.idExtractor,
        shardResolver = Post.shardResolver)

      if (port != "2551" && port != "2552")
        system.actorOf(Props[Bot], "bot")
    }

    def startupSharedJournal(system: ActorSystem, startStore: Boolean, path: ActorPath): Unit = {
      // Start the shared journal one one node (don't crash this SPOF)
      // This will not be needed with a distributed journal
      if (startStore)
        system.actorOf(Props[SharedLeveldbStore], "store")
      // register the shared journal
      import system.dispatcher
      implicit val timeout = Timeout(15.seconds)
      val f = (system.actorSelection(path) ? Identify(None))
      f.onSuccess {
        case ActorIdentity(_, Some(ref)) => SharedLeveldbJournal.setStore(ref, system)
        case _ =>
          system.log.error("Shared journal not started at {}", path)
          system.shutdown()
      }
      f.onFailure {
        case _ =>
          system.log.error("Lookup of shared journal at {} timed out", path)
          system.shutdown()
      }
    }

  }

}

