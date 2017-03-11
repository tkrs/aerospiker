import aerospiker._
import aerospiker.policy.{ ClientPolicy, WritePolicy }
import aerospiker.task.Aerospike
import cats.Foldable
import cats.data.Kleisli
import cats.syntax.applicativeError._
import cats.syntax.either._
import io.circe.generic.auto._
import monix.cats._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Await
import scala.concurrent.duration._

object Standard extends App {

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double], option: Option[String])

  val u1 = User("aaa", 31, System.currentTimeMillis(), Seq(1.2, 3.4), None)
  val u2 = User("bbb", 24, System.currentTimeMillis(), Seq(1.2, 3.4), Some("すごーい"))
  val u3 = User("ccc", 3, System.currentTimeMillis(), Seq(1.2, 3.4), Some("わーい"))

  val writePolicy = WritePolicy(sendKey = true, timeout = 3000, maxRetries = 5)
  val clientPolicy = ClientPolicy(writePolicyDefault = writePolicy)

  val client = AerospikeClient(clientPolicy, Host("127.0.0.1", 3000))

  val settings = Settings("test", "account")

  import Aerospike._

  val action: Kleisli[Task, AerospikeClient, List[User]] = for {
    _ <- put(settings, u1.name, u1)
    _ <- put(settings, u2.name, u2)
    _ <- put(settings, u3.name, u3)
    xs <- all[User](settings).map(_.toList.collect { case (_, Some(Record(Some(v), _, _))) => v })
    _ <- Foldable.iteratorFoldM[Action[Task, ?], User, Boolean](xs.iterator, true) {
      case (r, user) => delete(settings, user.name).map(_ && r)
      case _ => ??? //Kleisli.lift[Task, AerospikeClient, User](Task.raiseError(new Exception))
    }
  } yield xs

  Await.result(action.run(client).attempt.runAsync, 3.seconds).bimap(println, _.foreach(println))
  client.close()
}
