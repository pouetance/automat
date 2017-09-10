import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import hackernews._

import scala.util.{Failure, Success}

object Main {

  def main(args : Array[String]): Unit ={


    println("Fetching data from Hacker News. Please wait.")
    println("")
    println("")

    implicit val system = ActorSystem("actorsystem")
    implicit val materializer = ActorMaterializer()
    implicit val adapter = Logging(system, "customLogger")

    val http = Http()
    val reporter = hackernews.Reporter(http)
    val done = reporter.topStories().runForeach(println(_))

    implicit val ec = system.dispatcher
    done.onComplete{
      case Success(v) => system.terminate()
      case Failure(t) => println("An error has occured: " + t); system.terminate()
    }

  }

}

