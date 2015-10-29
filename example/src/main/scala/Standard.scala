import aerospiker._
import aerospiker.policy.{ WritePolicy, ClientPolicy }
import aerospiker.task.{ Settings, Aerospike }
import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.auto._

object Standard extends App with LazyLogging {

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double], option: Option[String])
  val u1 = User("bbb", 31, System.currentTimeMillis(), Seq(1.2, 3.4), None)
  val u2 = User("aaa", 24, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))
  val u3 = User("ccc", 3, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))

  val writePolicy = WritePolicy(sendKey = true, timeout = 3000, maxRetries = 5)
  val clientPolicy = ClientPolicy(writePolicyDefault = writePolicy)

  val client = AerospikeClient(clientPolicy, Host("192.168.99.100", 3000))

  val settings = Settings("test", "user", "u1")

  val action = for {
    put <- Aerospike.put(settings, u1)
    get <- Aerospike.get[User](settings)
    ret <- Aerospike.delete(settings)
  } yield ret

  println("start")
  println(action.run(client).attemptRun)

  client.close()
}
