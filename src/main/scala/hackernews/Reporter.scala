package hackernews

import akka.NotUsed
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import hackernews.Aggregator.Story

object Reporter {
  def apply(http : HttpExt)(implicit materializer : ActorMaterializer) : Reporter = {
    val api = new API(http)
    val aggregator = new Aggregator(api)
    new Reporter(api, aggregator)
  }
}

/**
  * A class creating reports related to the hacker news web site.
  */
class Reporter(api : API, aggregator : Aggregator) {

  val StoryPaddingLentgh = 140
  val ContributorPaddingLength = 50

  val ordinal = (v : Int) =>{
    val sufixes = "th" :: "st" :: "nd" :: "rd" :: List.fill(6)("th")
    (v % 100) match {
      case 11 => v + "th"
      case 12 => v + "th"
      case 13 => v + "th"
      case _  => v.toString + sufixes(v % 10)
    }
  }

  /**
    * A report on the contributions made by a commenter in the context of a given story.
    */
    case class ContributionReport(by : CommenterName, storyContributions : Int, totalContributions : Int)

    /**
     * A report of the top contributions to a given story.
     */
    case class StoryReport(text : String, topContributions : List[Option[ContributionReport]]){
      override def toString: String = {
        s"|${(" Story " + text).padTo(StoryPaddingLentgh, " ").mkString} |${topContributions.toList.map{v =>
          val string = v match{
            case Some(contributor) => s" ${contributor.by} (${contributor.storyContributions} for story - ${contributor.totalContributions} total)"
            case None => s" None"
          }
          string.padTo(ContributorPaddingLength, " ").mkString
        }.mkString("|")}"
      }
    }

  /**
    * A report of the top stories and contributions to Hacker News.
    */
    case class TopStoriesReport(stories : List[StoryReport] = List()){
      assert(stories.size > 0)

      def numberOfContributors : Int = stories(0).topContributions.size

      override def toString: String = {
        val headerStory : String  = "| Story".padTo(StoryPaddingLentgh + 2, " ").mkString
        val headerCommenters : Seq[String] =
          for (i <- (1 to numberOfContributors)) yield (s" ${ordinal(i)} Top Commenter".padTo(ContributorPaddingLength, " ")).mkString
        val headersRow = (headerStory :: headerCommenters.toList).mkString("|")

        val separatorsStory = "|" + List.fill(StoryPaddingLentgh + 1)("-").mkString
        val separatorsCommenter = List.fill(ContributorPaddingLength)("-").mkString
        val separatorsRow = (separatorsStory :: List.fill(numberOfContributors)(separatorsCommenter)).mkString("|")

        (headersRow :: separatorsRow :: stories).mkString("\n")
      }


    }

  /**
    * Create a source for a report concerning the top stories on hacker news.
    *
    * @param numberOfStories The number of stories we want the report to be based on.
    * @param numberOfContributorsPerStory The number of contributors on a given stories that we want stats on.
    * @return The source
    */
    def topStories(numberOfStories : Int = 30, numberOfContributorsPerStory : Int = 10 ) : Source[TopStoriesReport, NotUsed] = {
      assert(numberOfStories > 0)
      assert(numberOfStories <= 30)
      assert(numberOfContributorsPerStory > 0)

      def storyReport(storyDetails : Story, topStoriesContributions : Map[CommenterName, Int], numberOfContributorsToReport : Int) : StoryReport = {
        val topStoryContributors = storyDetails.contributions.toList.sortBy(v => v._2).reverse.take(numberOfContributorsToReport)

        val topContributionsInStory =
          for ((by, storyContributions) <- topStoryContributors)
            yield Some(ContributionReport(by = by, storyContributions = storyContributions, totalContributions = topStoriesContributions(by)))

        val completedtopContributionsInStory = topContributionsInStory.padTo(numberOfContributorsPerStory, None)

        StoryReport(text = storyDetails.title, completedtopContributionsInStory)
      }

      api.topStories()
        .mapConcat(topStories => topStories.take(numberOfStories).map(aggregator.aggregate(_)))
        .flatMapConcat(identity)
        .fold((List[Story](), Map[CommenterName, Int]())) ((current,newStory) => {
            val currentStories = current._1
            val currentTopStoriesContributions = current._2

            val newTopStoriesContributions = currentTopStoriesContributions ++ newStory.contributions.map{ case (k : String,v : Int) => k -> (v + currentTopStoriesContributions.getOrElse(k,0)) }

            (newStory :: currentStories, newTopStoriesContributions)
          })
        .map(value => {
          val stories = value._1
          val topStoriesContributions = value._2

          val storyReports =
            for {story <- stories}
              yield (storyReport(story, topStoriesContributions, numberOfContributorsPerStory))

          TopStoriesReport(stories = storyReports.reverse)
        })
    }


}
