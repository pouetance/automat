package hackernews

import akka.NotUsed
import akka.stream.scaladsl.Source
import hackernews.API.{ActiveCommentItem, DeadCommentItem, DeletedCommentItem, StoryItem}

object Aggregator{
  case class Story(title : String, id : ItemID, contributions : Map[CommenterName,Int])
}

import Aggregator._

/**
  * The aggregator class is in charge of getting all the information for one specific hacker news item.
  * This is done by calling hacker news api multiple times to get all pieces of data linked to the item.
  * @param api The hacker news API used to get all the data.
  */
class Aggregator(api : API) {

  val MaxNumberContributors = 100000
  /**
    * Create a source that aggregate all information regarding a specific hacker news story.
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
        case ActiveCommentItem(by, text, kids) => Source.single(by) :: kids.map(kid => contributorsUnderComment(kid))
        // we only show children of deleted and dead comments
        case DeletedCommentItem(kids) => kids.map(kid => contributorsUnderComment(kid))
        case DeadCommentItem(kids) => kids.map(kid => contributorsUnderComment(kid))
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
        .groupBy(MaxNumberContributors, identity)
        .map(_ -> 1)
        .reduce((l, r) => (l._1, l._2 + r._2))
        .mergeSubstreams
        .fold(Map[CommenterName, Int]())((l, e) => l + (e._1 -> e._2))
        .map(contributions => Story(title = story.title, id = story.id, contributions))
    }

    api.story(id).flatMapMerge(1, story => storyItemToStory(story))

  }
}
