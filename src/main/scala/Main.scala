import java.nio.file.Path

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ThrottleMode}
import com.typesafe.config.ConfigFactory
import spray.json.{DefaultJsonProtocol, DeserializationException, JsArray, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}
//import system.dispatcher // to get an implicit ExecutionContext into scope


object Main {

  case class FileToUpload(name: String, location: Path)


  trait Item {
    val by : String
    val kids : List[Int]
  }
  case class StoryItem(by : String, id : Int, kids : List[Int], title : String) extends Item
  case class CommentItem(by : String, text : String, kids : List[Int]) extends Item



  case class Story(title : String, id : Int, contributions : Map[String,Int]) {
    override def toString: String = {
      s"""${id} - ${title}
         | Top contributors : ${contributions.toList.sortBy(v => v._1).reverse.take(10).map(v => s"${v._1} (${v._2})").mkString(",")}
       """.stripMargin
    }
  }


  case class ContributionReport(by : String, storyContributions : Int, totalContributions : Int)
  case class StoryReport(text : String, topContributions : List[ContributionReport]){
    override def toString: String = {
      s"""${text}
         | Top contributors : ${topContributions.toList.sortBy(v => v.storyContributions).reverse.take(10).map(v => s"${v.by} ${v.storyContributions} (${v.totalContributions})").mkString(",")}
       """.stripMargin
    }
  }
  case class HackerNewReport(stories : List[StoryReport] = List()){
    override def toString: String = {
      stories.mkString("\n")
    }
  }


  trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
    //FIXME no kids....
    implicit val itemFormat = jsonFormat4(StoryItem)

    // we need to create a format to handle when the kids element is not present
    implicit object CommentFormat extends RootJsonFormat[Option[CommentItem]] {
      def write(value : Option[CommentItem]) = {
        value match{
          case Some(c) => JsObject("by" -> JsString(c.by), "text" -> JsString(c.text), "kids" -> JsArray(c.kids.map(JsNumber(_)).toVector))
          case None =>  JsObject()
        }

      }


      def read(value: JsValue) = {
        value.asJsObject.getFields("by","text", "kids") match {
          case Seq(JsString(by), JsString(text), JsArray(kids)) =>
            Some(CommentItem(by=by,text = text, kids = kids.map(_.convertTo[Int]).toList))
          case Seq(JsString(by),JsString(text)) =>  Some(CommentItem(by=by, text = text, kids = List()))
          case Seq() => None
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

    val poolClientFlow =
      http
        .cachedHostConnectionPoolHttps[String]("hacker-news.firebaseio.com")
        .throttle(1, 100 milliseconds, 1, ThrottleMode.shaping)
        .map{
          case (Success(response : HttpResponse), _) =>
            response
          case (Failure(ex), _) => throw new Exception("le cached pool fait chier")
        }

    def test (): Unit ={
      val done = topStories().runForeach(println(_))

      implicit val ec = system.dispatcher
      done.onComplete{
        case Success(v) => system.terminate()
        case Failure(t) => println("An error has occured: " + t); system.terminate()
      }

    }

    def storyToStoryReport(story : Story, totalContributions : Map[String, Int]) : StoryReport = {
      val topContributions = for ((by, storyContributions) <- story.contributions.toList.sortBy(v => v._2).reverse.take(10))
        yield (ContributionReport(by = by, storyContributions = storyContributions,totalContributions = totalContributions(by)))
      StoryReport(text = story.title, topContributions)
    }

    def topStories() : Source[HackerNewReport, NotUsed] = {
      val requestStory = HttpRequest(method = HttpMethods.GET, uri=s"https://hacker-news.firebaseio.com/v0/topstories.json")
      println(s"topstories : ${requestStory.uri}")

      val unmarshalF = (r : HttpResponse) => Unmarshal(r.entity).to[List[Int]]

      Source
        .single((requestStory,"topStories"))
        .via(poolClientFlow)
        .mapAsync(1)(unmarshalF)
        .mapConcat[Source[Story, NotUsed]](topStories => topStories.take(6).map(storySource(_)))
        .flatMapMerge(1,identity)
        .fold((List[Story](), Map[String, Int]()))((t,newStory) => {
          val stories = t._1
          val totalOccurences = t._2

          val newTotalOccurences = totalOccurences ++ newStory.contributions.map{ case (k : String,v : Int) => k -> (v + totalOccurences.getOrElse(k,0)) }

          (newStory :: stories, newTotalOccurences)
        })
        .map((t : (List[Story], Map[String, Int] )) => {
          val storyReports = for {story <- t._1} yield (storyToStoryReport(story, t._2))
          HackerNewReport(stories = storyReports)
        })
    }


   def reportSource(story : StoryItem) : Source[Story, NotUsed] = {
     Source
       .single(story)
       .mapConcat[Source[String, NotUsed]](item => item.kids.map(kid => commentSource(kid)))
       .flatMapMerge(1,identity)
       .groupBy(10000, identity)
       .map(_ -> 1)
       .reduce((l, r) => (l._1, l._2 + r._2))
       .mergeSubstreams
       .fold(Map[String, Int]())((l,e) => l + (e._1 -> e._2) )
       .map(contributions => Story(title = story.title, id = story.id, contributions))
   }

  def storySource(storyNumber : Int) : Source[Story, NotUsed] = {
    val requestStory = HttpRequest(method = HttpMethods.GET, uri=s"https://hacker-news.firebaseio.com/v0/item/${storyNumber}.json")
    println(s"story : ${requestStory.uri}")

    val unmarshalF = (r : HttpResponse) => Unmarshal(r.entity).to[StoryItem]

    Source
        .single((requestStory,s"story:${storyNumber}"))
       .via(poolClientFlow)
       .mapAsync(1)(unmarshalF)
       .flatMapMerge(1,story => reportSource(story))
  }

  //FIXME remove duplicate code
  def commentSource(storyNumber : Int) : Source[String, NotUsed] = {
    val requestStory = HttpRequest(method = HttpMethods.GET, uri=s"https://hacker-news.firebaseio.com/v0/item/${storyNumber}.json")

    val unmarshalF = (r : HttpResponse) => Unmarshal(r.entity).to[Option[CommentItem]]

    val source = Source
      .single((requestStory,s"comment:${storyNumber}"))
      .via(poolClientFlow)
      .mapAsync(1)(unmarshalF)
      .mapConcat[Source[String, NotUsed]]{
        case Some(item) => Source.single(item.by) :: item.kids.map(kid => commentSource(kid))
        case None => List()
      }
      .flatMapMerge(1,identity)

    source
  }

  }


  def main(args : Array[String]): Unit ={



    val test = new Test()
    test.test()
  }

}
