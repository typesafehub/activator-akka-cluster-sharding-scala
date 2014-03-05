package sample.blog

import java.io.File
import java.util.UUID
import scala.concurrent.duration._
import org.apache.commons.io.FileUtils
import com.typesafe.config.ConfigFactory
import akka.actor.ActorIdentity
import akka.actor.Identify
import akka.actor.Props
import akka.cluster.Cluster
import akka.contrib.pattern.ClusterSharding
import akka.persistence.Persistence
import akka.persistence.journal.leveldb.SharedLeveldbJournal
import akka.persistence.journal.leveldb.SharedLeveldbStore
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender

object BlogSpec extends MultiNodeConfig {
  val controller = role("controller")
  val node1 = role("node1")
  val node2 = role("node2")

  commonConfig(ConfigFactory.parseString("""
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.persistence.journal.plugin = "akka.persistence.journal.leveldb-shared"
    akka.persistence.journal.leveldb-shared.store {
      native = off
      dir = "target/test-shared-journal"
    }
    akka.persistence.snapshot-store.local.dir = "target/test-snapshots"
    """))
}

class BlogSpecMultiJvmNode1 extends BlogSpec
class BlogSpecMultiJvmNode2 extends BlogSpec
class BlogSpecMultiJvmNode3 extends BlogSpec

class BlogSpec extends MultiNodeSpec(BlogSpec)
  with STMultiNodeSpec with ImplicitSender {

  import BlogSpec._

  def initialParticipants = roles.size

  val storageLocations = List(
    "akka.persistence.journal.leveldb.dir",
    "akka.persistence.journal.leveldb-shared.store.dir",
    "akka.persistence.snapshot-store.local.dir").map(s => new File(system.settings.config.getString(s)))

  override protected def atStartup() {
    runOn(controller) {
      storageLocations.foreach(dir => FileUtils.deleteDirectory(dir))
    }
  }

  override protected def afterTermination() {
    runOn(controller) {
      storageLocations.foreach(dir => FileUtils.deleteDirectory(dir))
    }
  }

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address
      startSharding()
    }
    enterBarrier(from.name + "-joined")
  }

  def startSharding(): Unit = {
    ClusterSharding(system).start(
      typeName = AuthorListing.shardName,
      entryProps = Some(AuthorListing.props()),
      idExtractor = AuthorListing.idExtractor,
      shardResolver = AuthorListing.shardResolver)
    ClusterSharding(system).start(
      typeName = Post.shardName,
      entryProps = Some(Post.props(ClusterSharding(system).shardRegion(AuthorListing.shardName))),
      idExtractor = Post.idExtractor,
      shardResolver = Post.shardResolver)
  }

  "Sharded blog app" must {

    "setup shared journal" in {
      // start the Persistence extension
      Persistence(system)
      runOn(controller) {
        system.actorOf(Props[SharedLeveldbStore], "store")
      }
      enterBarrier("peristence-started")

      runOn(node1, node2) {
        system.actorSelection(node(controller) / "user" / "store") ! Identify(None)
        val sharedStore = expectMsgType[ActorIdentity].ref.get
        SharedLeveldbJournal.setStore(sharedStore, system)
      }

      enterBarrier("after-1")
    }

    "join cluster" in within(15.seconds) {
      join(node1, node1)
      join(node2, node1)
      enterBarrier("after-2")
    }

    "create, edit, and retrieve blog post" in within(15.seconds) {
      val postId = "45902e35-4acb-4dc2-9109-73a9126b604f"

      runOn(node1) {
        val postRegion = ClusterSharding(system).shardRegion(Post.shardName)
        postRegion ! Post.AddPost(postId,
          Post.PostContent("Patrik", "Sharding algorithms", "Splitting shards..."))
        postRegion ! Post.ChangeBody(postId,
          "Splitting shards across multiple...")
      }

      runOn(node2) {
        val postRegion = ClusterSharding(system).shardRegion(Post.shardName)
        awaitAssert {
          within(1.second) {
            postRegion ! Post.GetContent(postId)
            expectMsg(Post.PostContent("Patrik", "Sharding algorithms", "Splitting shards across multiple..."))
          }
        }
      }
      enterBarrier("after-3")
    }

    "publish and list blog post" in within(15.seconds) {

      runOn(node1) {
        val postRegion = ClusterSharding(system).shardRegion(Post.shardName)
        val postId = UUID.randomUUID().toString
        postRegion ! Post.AddPost(postId,
          Post.PostContent("Patrik", "Hash functions", "A hash function should be deterministic..."))
        postRegion ! Post.Publish(postId)
      }

      runOn(node2) {
        val listingRegion = ClusterSharding(system).shardRegion(AuthorListing.shardName)
        awaitAssert {
          within(1.second) {
            listingRegion ! AuthorListing.GetPosts("Patrik")
            val posts = expectMsgType[AuthorListing.Posts].list
            posts.isEmpty should be(false)
            posts.last.title should be("Hash functions")
          }
        }
      }
      enterBarrier("after-4")
    }

  }
}
