import aerospiker._
import aerospiker.task.syntax._
import aerospiker.policy.{ ClientPolicy, WritePolicy }
import aerospiker.task.AerospikeLargeMap
import io.circe.generic.auto._
import shapeless._, shapeless.syntax.singleton._, shapeless.record._

object LargeMap extends App {

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double])
  type Users = Map[String, User]
  val u1 = User("tkrs", 31, System.currentTimeMillis(), Seq(1.2, 3.4))
  val u2 = User("boo", 5, System.currentTimeMillis(), Seq(1.2, 3.4))
  val u3 = User("xyz", 99, System.currentTimeMillis(), Seq(1.00000002, 3.4))

  val writePolicy = WritePolicy(sendKey = true, timeout = 3000, maxRetries = 5)
  val clientPolicy = ClientPolicy(writePolicyDefault = writePolicy)
  val client = AerospikeClient(clientPolicy, Host("192.168.99.100", 3000))

  val settings = Settings("test", "user", "u1", "bin")

  import AerospikeLargeMap._
  val action = for {
    _ <- put(settings, "u1", u1)
    get <- get[User](settings, "u1")
    del <- delete(settings, "u1")
    _ <- puts(settings, Map(u1.name -> u1, u2.name -> u2, u3.name -> u3))
    all <- all[Users](settings)
    delBin <- deleteBin(settings)
  } yield "Done"

  println("start")
  println(action.run(client).attemptRun)

  client.close()
}
