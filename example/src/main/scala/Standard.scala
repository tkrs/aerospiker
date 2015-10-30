import java.util.concurrent.TimeUnit

import aerospiker._
import aerospiker.policy.{ ClientPolicy, WritePolicy }
import aerospiker.task.{ Aerospike, Settings }
import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.auto._
import org.openjdk.jmh.annotations.OutputTimeUnit

import scala.concurrent.duration.Duration

object Standard extends App with LazyLogging {

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double], option: Option[String])
  val u1 = User("bbb", 31, System.currentTimeMillis(), Seq(1.2, 3.4), None)
  val u2 = User("aaa", 24, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))
  val u3 = User("ccc", 3, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))

  val writePolicy = WritePolicy(sendKey = true, timeout = 3000, maxRetries = 5)
  val clientPolicy = ClientPolicy(writePolicyDefault = writePolicy)

  val client = AerospikeClient(clientPolicy, Host("192.168.99.100", 3000))

  val settings = Settings("test", "account", "user")

  import Aerospike._

  println(all[User](settings).run(client).attemptRunFor(Duration(1000, TimeUnit.MILLISECONDS)))

  val action = for {
    _ <- put(settings, u1)
    get <- get[User](settings)
    del <- delete(settings)
    _ <- puts(settings, Map(u1.name -> u1, u2.name -> u2, u3.name -> u3))
    all <- all[User](settings)
    act <- deletes(settings, Seq(u1.name, u2.name, u3.name))
  } yield {
    println(s"get :: $get")
    println(s"all :: $all")
    act
  }

  println("start")
  println(action.run(client).attemptRun)

  client.close()
}
