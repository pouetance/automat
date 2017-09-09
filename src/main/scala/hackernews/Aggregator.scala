package hackernews

import akka.NotUsed
import akka.stream.scaladsl.Source
import hackernews.API.StoryItem

object StoryAggregator{
  case class Story(title : String, id : ItemID, contributions : Map[CommenterName,Int])
}

import StoryAggregator._
class StoryAggregator(api : API) {

  /**
    * Create a source that aggregate all informations regarding a specific hacker news story.
    *
    * @param id The story id.
    * @return The created source
    */
  def aggregate(id : ItemID) : Source[Story, NotUsed] = {

    /** Return all the commenter names that have contributions under a given comment.
      * Note that contributors commenting more than one time will be emitted by the source for each of their comment.
      */
    def contributorsUnderComment(id: ItemID): Source[CommenterName, NotUsed] = {
      api
        .comment(id)
        .mapConcat[Source[CommenterName, NotUsed]] {
        case Some(item) => Source.single(item.by) :: item.kids.map(kid => contributorsUnderComment(kid))
        case None => List()
      }
        .flatMapMerge(1, identity)
    }

    /** Create a source in charge of retrieving and aggregating all the information according
      * to an already retrieved storyItem
      */
    def storyItemToStory(story: StoryItem): Source[Story, NotUsed] = {
      Source
        .single(story)
        .mapConcat[Source[CommenterName, NotUsed]](item => item.kids.map(kid => contributorsUnderComment(kid)))
        .flatMapMerge(1, identity)
        .groupBy(10000, identity)
        .map(_ -> 1)
        .reduce((l, r) => (l._1, l._2 + r._2))
        .mergeSubstreams
        .fold(Map[CommenterName, Int]())((l, e) => l + (e._1 -> e._2))
        .map(contributions => Story(title = story.title, id = story.id, contributions))
    }

    api.story(id).flatMapMerge(1, story => storyItemToStory(story))

  }
}
