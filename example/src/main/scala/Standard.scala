import aerospiker.policy.{ WritePolicy, ClientPolicy }
import cats.data.Xor._
import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.auto._

import scala.collection.mutable

object Standard extends App with LazyLogging {

  import aerospiker._
  import aerospiker.task.Aerospike

  val writePolicy = WritePolicy(sendKey = true, timeout = 3000, maxRetries = 5)
  implicit val clientPolicy = ClientPolicy(writePolicyDefault = writePolicy)

  val client = AsyncClient(Host("192.168.99.100", 3000))
  logger.info(s"connected => ${client.cluster.isConnected}")
  for (node <- client.cluster.getNodes) {
    logger.info(node.getHost.toString)
    logger.info(node.getName.toString)
  }

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double], option: Option[String])
  val u1 = User("bbb", 31, System.currentTimeMillis(), Seq(1.2, 3.4), None)
  val u2 = User("aaa", 24, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))
  val u3 = User("ccc", 3, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))

  val cmd = AsyncCommandExecutor(client)

  val task = new Aerospike(cmd) {
    override protected def namespace: String = "test"
    override protected def setName: String = "user"
  }

  val key1 = "u1"
  val key2 = "u2"
  val key3 = "test"

  task.put[User](key1, u1).run match {
    case Right(v) => logger.info(s"put $v")
    case Left(e) => e.printStackTrace()
  }

  task.put[User](key2, u2).run match {
    case Right(v) => logger.info(s"put $v")
    case Left(e) => e.printStackTrace()
  }

  task.put[User](key3, u3).run match {
    case Right(v) => logger.info(s"put $v")
    case Left(e) => e.printStackTrace()
  }

  task.get[User](key1).run match {
    case Right(v) => logger.info(s"get $v")
    case Left(e) => e.printStackTrace()
  }

  task.get[User](key2).run match {
    case Right(v) => logger.info(s"get $v")
    case Left(e) => e.printStackTrace()
  }

  task.all[User]("name", "age", "now", "bbb").run match {
    case Right(v) => logger.info(s"all $v")
    case Left(e) => e.printStackTrace()
  }

  task.delete(key3).run match {
    case Right(v) => logger.info(s"delete $v")
    case Left(e) => e.printStackTrace()
  }

  task.deletes(key1 :: key2 :: Nil).run.foreach {
    case Right(v) => logger.info(s"delete $v")
    case Left(e) => e.printStackTrace()
  }

  val m = mutable.Map.empty[String, User]
  (1 to 10000) map (_ -> u1) foreach (i => m += i.toString -> u1)

  task.puts(m.toMap).run.foreach {
    case Right(v) => logger.info(s"put $v")
    case Left(e) => e.printStackTrace()
  }

  task.deletes(m.keys.toSeq).run.foreach {
    case Right(v) => logger.info(s"delete $v")
    case Left(e) => e.printStackTrace()
  }

  client.cluster.close()

}
