import aerospiker._
import aerospiker.policy.{ WritePolicy, ClientPolicy }
import aerospiker.task.{ Settings, AerospikeLargeMap }
import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.auto._
import io.circe.Decoder

object LargeMap extends App with LazyLogging {

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double])
  type Users = Map[String, User]

  val u1 = User("tkrs", 31, System.currentTimeMillis(), Seq(1.2, 3.4))
  val u2 = User("boo", 5, System.currentTimeMillis(), Seq(1.2, 3.4))
  val u3 = User("xyz", 99, System.currentTimeMillis(), Seq(1.00000002, 3.4))

  val writePolicy = WritePolicy(sendKey = true, timeout = 3000, maxRetries = 5)
  val clientPolicy = ClientPolicy(writePolicyDefault = writePolicy)
  val client = AerospikeClient(clientPolicy, Host("192.168.99.100", 3000))

  val settings = Settings("test", "user", "u1", "record")

  val action = for {
    _ <- AerospikeLargeMap.put(settings, "u1", u1)
    get <- AerospikeLargeMap.get[User](settings, "u1")
    del <- AerospikeLargeMap.delete(settings, "u1")
  } yield del

  println("start")
  println(action.run(client).attemptRun)

  client.close()
}
