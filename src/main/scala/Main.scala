import java.nio.file.Path

import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, _}

import scala.concurrent.ExecutionContext.Implicits.global
import akka.http.scaladsl.model.Multipart.FormData
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import spray.json.{DefaultJsonProtocol, DeserializationException, JsArray, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}


//import system.dispatcher // to get an implicit ExecutionContext into scope


object Main {

  case class FileToUpload(name: String, location: Path)


  trait Item {
    val by : String
    val kids : List[Int]
  }
  case class Story(by : String, id : Int, kids : List[Int], title : String) extends Item
  case class Comment(by : String, text : String,  kids : List[Int]) extends Item

  case class Contribution(by : String, occurrences : Int)
  case class StoryReport(title : String, id : Int, contributions : List[Contribution]) {
    override def toString: String = {
      s"""${id} - ${title}
         | Top contributors : ${contributions.sortBy(v => v.occurrences).reverse.take(3).map(v => s"${v.by} (${v.occurrences})").mkString(",")}
       """.stripMargin
    }


  }


  trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
    implicit val itemFormat = jsonFormat4(Story)

    // we need to create a format to handle when the kids element is not present
    implicit object CommentFormat extends RootJsonFormat[Comment] {
      def write(c: Comment) =
        JsObject("by" -> JsString(c.by), "text" -> JsString(c.text), "kids" -> JsArray(c.kids.map(JsNumber(_)).toVector))

      def read(value: JsValue) = {
        value.asJsObject.getFields("by","text", "kids") match {
          case Seq(JsString(by), JsString(text), JsArray(kids)) =>
            new Comment(by=by,text = text, kids = kids.map(_.convertTo[Int]).toList)
          case Seq(JsString(by),JsString(text)) =>  new Comment(by=by, text = text, kids = List())
          //FIXME uses an option instead
          case Seq() => new Comment(by="deleted", text = "deleted", kids = List())
          case e => throw new DeserializationException(s"Comment expected. Got ${e}")
        }
      }
    }
  }


  class Test extends JsonSupport{
    implicit val system = ActorSystem("aaa",
      ConfigFactory.parseString(
        s"""
           |akka {
           |    http {
           |
           |        host-connection-pool {
           |            max-connections = 1024
           |            max-open-requests = 1024
           |            max-connections = 1024
           |        }
           |    }
           |}
             """.stripMargin))
    implicit val materializer = ActorMaterializer()
    val http = Http()

    def test (): Unit ={


      val done = topStories()


      implicit val ec = system.dispatcher
      done.onComplete{
        case Success(v) => system.terminate()
        case Failure(t) => println("An error has occured: " + t.getMessage); system.terminate()

      }


      //val topStories = HttpRequest(method = HttpMethods.GET, uri="https://hacker-news.firebaseio.com/v0/topstories.json")
      //http.singleRequest(topStories).onComplete(onCompleteTopStories)


    }

    def topStories() : Source[StoryReport, NotUsed] = {
      val requestStory = HttpRequest(method = HttpMethods.GET, uri=s"https://hacker-news.firebaseio.com/v0/topstories.json?print=pretty")
      println(s"topstories : ${requestStory.uri}")

      val unmarshalF = (r : HttpResponse) => Unmarshal(r.entity).to[List[Int]]

      Source
        .fromFuture(http.singleRequest(requestStory))
        .mapAsync(1)(unmarshalF)
        .mapConcat[Source[StoryReport, NotUsed]](topStories => /*topStories.take(1).map(storySource(_))*/ List(storySource(topStories(10))))
        .flatMapConcat(identity)

    }


   def reportSource(story : Story) : Source[StoryReport, NotUsed] = {
     Source
       .single(story)
       .mapConcat[Source[String, NotUsed]](item => item.kids.map(kid => commentSource(kid)))
       .flatMapConcat(identity)
       .groupBy(10000, identity)
       .map(_ -> 1)
       .reduce((l, r) => (l._1, l._2 + r._2))
       .mergeSubstreams
       .fold(List[Contribution]())((l,e) => Contribution(by = e._1, occurrences = e._2) :: l)
       .map(contributions => StoryReport(title = story.title, id = story.id, contributions))
   }

  def storySource(storyNumber : Int) : Source[StoryReport, NotUsed] = {
    val requestStory = HttpRequest(method = HttpMethods.GET, uri=s"https://hacker-news.firebaseio.com/v0/item/${storyNumber}.json?print=pretty")
    println(s"story : ${requestStory.uri}")

    val unmarshalF = (r : HttpResponse) => Unmarshal(r.entity).to[Story]

    Source
       .fromFuture(http.singleRequest(requestStory))
       .mapAsync(1)(unmarshalF)
       .flatMapConcat(story => reportSource(story))
  }

  //FIXME remove duplicate code
  def commentSource(storyNumber : Int) : Source[String, NotUsed] = {
    val requestStory = HttpRequest(method = HttpMethods.GET, uri=s"https://hacker-news.firebaseio.com/v0/item/${storyNumber}.json?print=pretty")

    val unmarshalF = (r : HttpResponse) => Unmarshal(r.entity).to[Comment]

    val source = Source
      .fromFuture(http.singleRequest(requestStory))
      .mapAsync(1)(unmarshalF)
      .mapConcat[Source[String, NotUsed]](item => Source.single(item.by) :: item.kids.map(kid => commentSource(kid)))
      .flatMapConcat(identity)

    source
  }

  }


  def main(args : Array[String]): Unit ={



    val test = new Test()
    test.test()
  }

}
