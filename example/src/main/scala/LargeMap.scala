import cats.data.Xor._
import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.auto._

object LargeMap extends App with LazyLogging {

  import aerospiker._
  import AsyncClient._
  import task.AerospikeLargeMap

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double])
  type Users = Map[String, User]

  val u1 = User("tkrs", 31, System.currentTimeMillis(), Seq(1.2, 3.4))
  val u2 = User("boo", 5, System.currentTimeMillis(), Seq(1.2, 3.4))
  val u3 = User("xyz", 99, System.currentTimeMillis(), Seq(1.00000002, 3.4))

  val client = AsyncClient(Host("192.168.59.103", 3000))
  val cmd = AsyncCommandExecutor(client)

  val lmap = new AerospikeLargeMap(cmd) {
    override protected def namespace: String = "test"
    override protected def setName: String = "large"
    override protected def key: String = "user"
    override protected def binName: String = "list"
  }

  lmap.put("u1", u1).run match {
    case Left(e) => e.printStackTrace()
    case Right(v) => logger.info(v.toString)
  }

  lmap.get[User]("u1").run match {
    case Left(e) => e.printStackTrace()
    case Right(v) => logger.info(v.toString)
  }

  lmap.puts(Map("u2" -> u1, "u1" -> u2)).run match {
    case Left(e) => e.printStackTrace()
    case Right(v) => logger.info(v.toString)
  }

  lmap.put("u3", u3).run match {
    case Left(e) => e.printStackTrace()
    case Right(v) => logger.info(v.toString)
  }

  lmap.all[Users].run match {
    case Left(e) => e.printStackTrace()
    case Right(v) => logger.info(v.toString)
  }

  lmap.delete("u2").run match {
    case Left(e) => e.printStackTrace()
    case Right(v) => logger.info(v.toString)
  }

  lmap.all[Users].run match {
    case Left(e) => e.printStackTrace()
    case Right(v) => logger.info(v.toString)
  }

  lmap.deleteBin.run match {
    case Left(e) => e.printStackTrace()
    case Right(v) => logger.info(v.toString)
  }

  lmap.all[Users].run match {
    case Left(e) => e.printStackTrace()
    case Right(v) => logger.info(v.toString)
  }
}
