package aerospiker
package task.monix

import _root_.monix.execution.Scheduler
import aerospiker.policy.ClientPolicy
import cats.syntax.option._
import cats.syntax.either._
import com.aerospike.client.async.AsyncCluster
import io.circe.{ Decoder, Encoder, Json }
import org.scalatest.{ FunSpec, Matchers }

import scala.collection.generic.CanBuildFrom
import scala.concurrent.Await
import scala.concurrent.duration._

class AerospikeSpec extends FunSpec with Matchers {

  val x = new Aerospike.Impl with Functions {
    protected def getAsync[A: Decoder](
      fa: Func[A],
      settings: Settings,
      binNames: String*
    )(implicit c: AerospikeClient): Unit =
      fa(Decoder[A].decodeJson(Json.fromInt(10)))

    protected def putAsync[A: Encoder](
      fa: Func[Unit],
      settings: Settings, bins: A
    )(implicit c: AerospikeClient): Unit =
      fa(().asRight)

    protected def deleteAsync(
      fa: Func[Boolean],
      settings: Settings
    )(implicit c: AerospikeClient): Unit =
      fa(true.asRight)

    protected def allAsync[C[_], A: Decoder](
      fa: Func[C[(Key, Option[Record[A]])]],
      settings: Settings,
      binNames: String*
    )(
      implicit
      c: AerospikeClient,
      cbf: CanBuildFrom[Nothing, (Key, Option[Record[A]]), C[(Key, Option[Record[A]])]]
    ): Unit = {
      fa(Vector(
        Key.apply("", "", "") -> Record.apply(10.some, 0, 0).some,
        Key.apply("", "", "") -> Record.apply(20.some, 0, 0).some
      ).asInstanceOf[C[(Key, Option[Record[A]])]].asRight)
    }

    protected def existsAsync(
      fa: Func[Boolean],
      settings: Settings
    )(implicit c: AerospikeClient): Unit =
      fa(true.asRight)

  }

  val c: AerospikeClient = new AerospikeClient {
    override def cluster: AsyncCluster = ???
    override def close(): Unit = ???
    override def policy: ClientPolicy = ???
  }

  val settings: Settings = Settings("", "", "", "")

  describe("get") {
    it("should return a value with arbitrary type") {
      implicit val sc = Scheduler.global
      val t = x.get[Int](settings, "a").run(c)
      val r = Await.result(t.runAsync, 3.seconds)
      assert(r === 10)
    }
  }

  describe("put") {
    it("should successfully") {
      implicit val sc = Scheduler.global
      val t = x.put[Int](settings, 20).run(c)
      Await.result(t.runAsync, 3.seconds)
    }
  }

  describe("delete") {
    it("should return true") {
      implicit val sc = Scheduler.global
      val t = x.delete(settings).run(c)
      val r = Await.result(t.runAsync, 3.seconds)
      assert(r === true)
    }
  }

  describe("all") {
    it("should return true") {
      implicit val sc = Scheduler.global
      val t = x.all[Vector, Int](settings, "x").run(c)
      val r = Await.result(t.runAsync, 3.seconds)
      val expected = Vector(
        Key.apply("", "", "") -> Record.apply(10.some, 0, 0).some,
        Key.apply("", "", "") -> Record.apply(20.some, 0, 0).some
      )
      assert(r === expected)
    }
  }

  describe("exists") {
    it("should return true") {
      implicit val sc = Scheduler.global
      val t = x.exists(settings).run(c)
      val r = Await.result(t.runAsync, 3.seconds)
      assert(r === true)
    }
  }
}
