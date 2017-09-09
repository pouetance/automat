import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

package object hackernews {

  type ItemID = Int
  type CommenterName = String
  implicit val system = ActorSystem("actorsystem")
  implicit val materializer = ActorMaterializer()

}
