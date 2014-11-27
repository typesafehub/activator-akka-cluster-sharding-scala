package sample.blog

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import com.fasterxml.jackson.dataformat.cbor.CBORFactory
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator
import com.fasterxml.jackson.core.io.SerializedString
import com.fasterxml.jackson.dataformat.cbor.CBORParser
import java.io.ByteArrayOutputStream
import scala.collection.immutable

trait BlogData extends Serializable

class BlogSerializer(val system: ExtendedActorSystem) extends Serializer {

  import Post._
  import AuthorListing._

  override def includeManifest: Boolean = false
  override def identifier = 12345

  private val factory = new CBORFactory
  private val typeFieldName = new SerializedString("type")
  private val authorFieldName = new SerializedString("author");
  private val titleFieldName = new SerializedString("title");
  private val bodyFieldName = new SerializedString("body");
  private val postIdFieldName = new SerializedString("postId");

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case evt: PostAdded   => postAddedToBinary(evt)
    case evt: PostSummary => postSummaryToBinary(evt)
    case _ ⇒
      throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    val parser = factory.createParser(bytes)
    parser.nextFieldName(typeFieldName)
    parser.nextIntValue(-1) match {
      case 1 => parsePostAdded(parser)
      case 2 => parsePostSummary(parser)
      case -1 => throw new IllegalArgumentException(
        s"Unimplemented deserialization of type ${parser.getText} in ${getClass.getName}")
    }
  }

  private def postAddedToBinary(evt: PostAdded): Array[Byte] = {
    val out = new ByteArrayOutputStream(1024)
    val gen = factory.createGenerator(out)
    gen.writeNumberField(typeFieldName.getValue, 1)
    gen.writeStartObject()
    gen.writeStringField(authorFieldName.getValue, evt.content.author)
    gen.writeStringField(titleFieldName.getValue, evt.content.title)
    gen.writeStringField(bodyFieldName.getValue, evt.content.body)
    gen.writeEndObject()
    gen.close()
    out.toByteArray()
  }

  private def parsePostAdded(parser: CBORParser): PostAdded = {
    parser.nextToken()
    require(parser.nextFieldName(authorFieldName))
    val author = parser.nextTextValue()
    require(parser.nextFieldName(titleFieldName))
    val title = parser.nextTextValue()
    require(parser.nextFieldName(bodyFieldName))
    val body = parser.nextTextValue()
    PostAdded(PostContent(author, title, body))
  }

  private def postSummaryToBinary(evt: PostSummary): Array[Byte] = {
    val out = new ByteArrayOutputStream(1024)
    val gen = factory.createGenerator(out)
    gen.writeNumberField(typeFieldName.getValue, 2)
    gen.writeStartObject()
    gen.writeStringField(authorFieldName.getValue, evt.author)
    gen.writeStringField(postIdFieldName.getValue, evt.postId)
    gen.writeStringField(titleFieldName.getValue, evt.title)
    gen.writeEndObject()
    gen.close()
    out.toByteArray()
  }

  private def parsePostSummary(parser: CBORParser): PostSummary = {
    parser.nextToken()
    require(parser.nextFieldName(authorFieldName))
    val author = parser.nextTextValue()
    require(parser.nextFieldName(postIdFieldName))
    val postId = parser.nextTextValue()
    require(parser.nextFieldName(titleFieldName))
    val title = parser.nextTextValue()
    PostSummary(author, postId, title)
  }

}
