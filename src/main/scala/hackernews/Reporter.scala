package hackernews

import akka.NotUsed
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.scaladsl.Source
import hackernews.StoryAggregator.Story

object Reporter {
  def instance(http : HttpExt) : Reporter = {
    val api = new API(http)
    val aggregator = new StoryAggregator(api)
    new Reporter(api, aggregator)
  }
}

class Reporter(api : API, aggregator : StoryAggregator) {

  /**
    * A report on the contributions made by a commenter in the context of a given story.
    */
    case class ContributionReport(by : CommenterName, storyContributions : Int, totalContributions : Int)

    /**
     * A report of the top contributions to a given story.
     */
    case class StoryReport(text : String, topContributions : List[ContributionReport]){
      override def toString: String = {
        s"|Story ${text} | ${topContributions.toList.sortBy(v => v.storyContributions).reverse.take(10).map(v => s"${v.by} ${v.storyContributions} (${v.totalContributions})").mkString("|")}"
      }
    }

  /**
    * A report of the top stories and contributions to Hacker News.
    */
    case class TopStoriesReport(stories : List[StoryReport] = List()){
      override def toString: String = {
        s"""
           |${stories.mkString("\n")}
         """.stripMargin
      }
    }

  /**
    * Create a source for a report concerning the top stories on hacker news.
    *
    * @param numberOfStories The number of stories we want the report to be based on.
    * @param numberOfContributorsPerStory The number of contributors on a given stories that we want stats on.
    * @return The source
    */
    def topStories(numberOfStories : Int = 3, numberOfContributorsPerStory : Int = 10 ) : Source[TopStoriesReport, NotUsed] = {

      def storyReport(storyDetails : Story, topStoriesContributions : Map[CommenterName, Int], numberOfContributorsToReport : Int) : StoryReport = {
        val topStoryContributors = storyDetails.contributions.toList.sortBy(v => v._2).reverse.take(numberOfContributorsToReport)

        val topContributionsInStory =
          for ((by, storyContributions) <- topStoryContributors)
            yield (ContributionReport(by = by, storyContributions = storyContributions, totalContributions = topStoriesContributions(by)))
        StoryReport(text = storyDetails.title, topContributionsInStory)
      }

      api.topStories()
        .mapConcat(topStories => topStories.take(numberOfStories).map(aggregator.aggregate(_)))
        .flatMapMerge(1,identity)
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
