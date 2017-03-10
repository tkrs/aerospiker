import aerospiker._
import aerospiker.policy.{ ClientPolicy, WritePolicy }
import aerospiker.task.Aerospike
import io.circe.generic.auto._
import monix.cats._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Await
import scala.concurrent.duration._

object Standard extends App {

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double], option: Option[String])
  val u1 = User("bbb", 31, System.currentTimeMillis(), Seq(1.2, 3.4), None)
  val u2 = User("aaa", 24, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))
  val u3 = User("ccc", 3, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))

  val writePolicy = WritePolicy(sendKey = true, timeout = 3000, maxRetries = 5)
  val clientPolicy = ClientPolicy(writePolicyDefault = writePolicy)

  val client = AerospikeClient(clientPolicy, Host("127.0.0.1", 3000))

  val settings = Settings("test", "account", "user")

  import Aerospike._

  try {
    val action = for {
      _ <- put(settings, u1)
      get <- get[User](settings)
      del <- delete(settings)
      // _ <- puts(settings, Map(u1.name -> u1, u2.name -> u2, u3.name -> u3))
      all <- all[User](settings)
      // dels <- deletes(settings, Seq(u1.name, u2.name, u3.name))
    } yield "Done"

    println("start 1")
    println(Await.result(action.run(client).runAsync, 3.seconds))
  } catch {
    case e: Throwable => println(e.getMessage)
  }

  try {
    val action1 = for {
      put <- put(settings, u1)
      get <- get[User](settings)
    } yield get

    val action2 = for {
      put <- put(settings, u2)
      get <- get[User](settings)
    } yield get

    val t1 = Task.fork(action1.run(client))
    val t2 = Task.fork(action2.run(client))

    println("start 2")
    println(Await.result(Task.mapBoth(t1, t2)((a, b) => (a, b)).runAsync, 3.seconds))
  } catch {
    case e: Throwable => println(e.getMessage)
  }

  client.close()
}
