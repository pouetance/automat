import akka.http.scaladsl.Http
import hackernews._

import scala.util.{Failure, Success}

object Main {

  def main(args : Array[String]): Unit ={
    val http = Http()
    val reporter = hackernews.Reporter.instance(http)
    val done = reporter.topStories().runForeach(println(_))

    implicit val ec = system.dispatcher
    done.onComplete{
      case Success(v) => system.terminate()
      case Failure(t) => println("An error has occured: " + t); system.terminate()
    }

  }

}
