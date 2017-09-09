package hackernews

import akka.NotUsed
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Source
import hackernews.API.{CommentItem, JsonSupport, StoryItem}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsArray, JsBoolean, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object API {


  /*
  The base trait for any item retrieved through the hacker news API (comment or story)
   */
  trait ApiItem {
    /* The item author*/
    val by: CommenterName
    /** The item descendants. */
    val kids: List[Int]
  }

  /**
    * Represents a story as returned by the hacker news API.
    *
    * @param by    The story author
    * @param id    The story id.
    * @param kids  The story descendants which are the top level comments
    * @param title The story title.
    */
  case class StoryItem(by: CommenterName, id: ItemID, kids: List[ItemID], title: String) extends ApiItem

  /**
    * Represents a comment as returned by the hacker news API
    *
    * @param by   The comment author.
    * @param text The comment text.
    * @param kids The comment descendants which are the answer to this commment.
    */
  case class CommentItem(by: CommenterName, text: String, kids: List[ItemID]) extends ApiItem


  trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

    /**
      * The json format used by the hacker news API to represent a story item.
      * We can't use a more automated method since we need to handle some tags not always being present.
      */
    implicit object StoryFormat extends RootJsonFormat[StoryItem] {
      def write(value: StoryItem) = {
        JsObject("by" -> JsString(value.by), "id" -> JsNumber(value.id), "kids" -> JsArray(value.kids.map(JsNumber(_)).toVector), "title" -> JsString("title"))
      }


      def read(value: JsValue) = {
        value.asJsObject.getFields("by", "id", "kids", "title") match {
          case Seq(JsString(by), JsNumber(id), JsArray(kids), JsString(title)) =>
            StoryItem(by = by, id = id.toInt, kids = kids.map(_.convertTo[Int]).toList, title = title)
          // when there are no descendants, the 'kids' field is not present
          case Seq(JsString(by), JsNumber(id), JsString(title)) => StoryItem(by = by, id = id.toInt, kids = List(), title = title)
          case e => throw new DeserializationException(s"Unable to read story item : ${value.prettyPrint}")
        }
      }
    }

    /**
      * The json format used by the hacker news API to represent a comment item.
      * We can't use a more automated method since we need to handle some tags not always being present
      * and deleted comments
      */
    implicit object CommentFormat extends RootJsonFormat[Option[CommentItem]] {
      def write(value: Option[CommentItem]) = {
        value match {
          case Some(c) => JsObject("by" -> JsString(c.by), "text" -> JsString(c.text), "kids" -> JsArray(c.kids.map(JsNumber(_)).toVector))
          case None => JsObject()
        }

      }


      def read(value: JsValue) = {
        value.asJsObject.getFields("by", "text", "kids", "deleted") match {
          case Seq(JsString(by), JsString(text), JsArray(kids)) =>
            Some(CommentItem(by = by, text = text, kids = kids.map(_.convertTo[Int]).toList))
          case Seq(JsString(by), JsString(text)) => Some(CommentItem(by = by, text = text, kids = List()))
          //deleted comments handling
          case Seq(_,JsBoolean(true)) => None
          case Seq(JsBoolean(true)) => None
          case e => throw new DeserializationException(s"Unable to read comment item : ${value.prettyPrint}")
        }
      }
    }

  }

}


class API(val http: HttpExt) extends JsonSupport{

  val httpRequestSource =
    (request : HttpRequest, name : String) => {
      Source
        .single(request,name).via(
        http
          .cachedHostConnectionPoolHttps[String]("hacker-news.firebaseio.com")
          // we throttle the incoming requests in case hacker news has some DOS service protections
          .throttle(1, 100 milliseconds, 1, ThrottleMode.shaping)
          .map {
            case (Success(response: HttpResponse), _) =>
              response
            case (Failure(ex), _) => throw new Exception(s"Http pool exception ${ex}")
          }
      )
    }
    val itemRequest = (itemID : ItemID) => HttpRequest(method = HttpMethods.GET, uri=s"https://hacker-news.firebaseio.com/v0/item/${itemID}.json")

    def topStories() : Source[List[ItemID], NotUsed] = {
      val requestStory = HttpRequest(method = HttpMethods.GET, uri=s"https://hacker-news.firebaseio.com/v0/topstories.json")
      val unmarshalFunction = (r : HttpResponse) => Unmarshal(r.entity).to[List[Int]]

      httpRequestSource(requestStory,"topStories")
        .mapAsync(1)(unmarshalFunction)
    }

    def story(storyId : ItemID) : Source[StoryItem, NotUsed] = {
      val requestStory = itemRequest(storyId)
      val unmarshalFunction = (r : HttpResponse) => Unmarshal(r.entity).to[StoryItem]


      httpRequestSource(requestStory,s"story:${storyId}")
        .mapAsync(1)(unmarshalFunction)
    }

    def comment(commentId : ItemID) : Source[Option[CommentItem], NotUsed] = {
      val requestStory = itemRequest(commentId)
      val unmarshalFunction = (r : HttpResponse) => Unmarshal(r.entity).to[Option[CommentItem]]

      httpRequestSource(requestStory,s"comment:${commentId}")
        .mapAsync(1)(unmarshalFunction)
    }

}




