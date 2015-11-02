package aerospiker
package task

import aerospiker.command.UdfExecute
import aerospiker.data.Record
import aerospiker.listener._
import aerospiker.policy._
import com.aerospike.client.AerospikeException
import com.aerospike.client.async.AsyncCluster
import io.circe.{ Encoder, Decoder }
import shapeless._

import scalaz.{ \/-, -\/ }
import scalaz.concurrent.Task

object AerospikeLargeMap {

  import Command._

  private[this] val PackageName = "lmap"

  private[this] def execute[A, R](cluster: AsyncCluster, policy: WritePolicy, listener: Option[ExecuteListener[R]], key: Key, packageName: String, functionName: String, functionArgs: A)(
    implicit
    encoder: Encoder[A],
    decoder: Decoder[R]
  ) =
    new UdfExecute[A, R](cluster, policy, listener, key, packageName, functionName, functionArgs).execute()

  def get[R](settings: Settings, a: String)(
    implicit
    decoder: Decoder[R]
  ) = withClient { client =>
    implicit val dec = Decoder[Map[String, Map[String, R]]]
    Task.async[Option[R]] { cb =>
      try {
        execute(
          client.cluster,
          client.policy.asyncWritePolicyDefault,
          Some(new ExecuteListener[Map[String, Map[String, R]]] {
            override def onFailure(e: AerospikeException): Unit = cb(-\/(e))

            override def onSuccess(key: Key, rec: Option[Record[Map[String, Map[String, R]]]]): Unit = {
              val o = rec.collect {
                case Record(Some(x), _, _) => x
              }
              val x = o getOrElse Map.empty
              val v = x.get("SUCCESS") flatMap (_ get a)
              cb(\/-(v))
            }
          }),
          Key(settings.namespace, settings.setName, settings.key),
          PackageName,
          "get",
          settings.binName :: a :: Nil
        )
      } catch {
        case e: Throwable => -\/(GetError(settings.binName, e))
      }
    }
  }

  def puts[A](settings: Settings, m: A)(
    implicit
    encoder: Encoder[String :: A :: HNil]
  ) = withClient { client =>
    Task.async[Unit] { cb =>
      try {
        execute[String :: A :: HNil, Map[String, Int]](
          client.cluster,
          client.policy.asyncWritePolicyDefault,
          Some(new ExecuteListener[Map[String, Int]] {
            override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
            override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
              val o = a.collect {
                case Record(Some(x), _, _) => x
              }
              val x = o getOrElse Map.empty
              if (x.contains("SUCCESS"))
                cb(\/-({}))
              else if (x.contains("FAILURE"))
                cb(-\/(new AerospikeException(a.toString)))
              else
                cb(-\/(new AerospikeException("Invalid UDF return value")))
            }
          }),
          Key(settings.namespace, settings.setName, settings.key),
          PackageName,
          "put_all",
          settings.binName :: m :: HNil
        )
      } catch {
        case e: Throwable => cb(-\/(PutError(settings.binName, e)))
      }
    }
  }

  def put[A](settings: Settings, name: String, value: A)(
    implicit
    encoder: Encoder[String :: String :: A :: HNil]
  ) = withClient { client =>
    Task.async[Unit] { cb =>
      try {
        execute[String :: String :: A :: HNil, Map[String, Int]](
          client.cluster,
          client.policy.asyncWritePolicyDefault,
          Some(new ExecuteListener[Map[String, Int]] {
            override def onFailure(e: AerospikeException): Unit = cb(-\/(e))

            override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
              val o = a.collect {
                case Record(Some(x), _, _) => x
              }
              val x = o getOrElse Map.empty
              if (x.contains("SUCCESS"))
                cb(\/-({}))
              else if (x.contains("FAILURE"))
                cb(-\/(new AerospikeException(a.toString)))
              else
                cb(-\/(new AerospikeException("Invalid UDF return value")))
            }
          }),
          Key(settings.namespace, settings.setName, settings.key),
          PackageName,
          "put",
          settings.binName :: name :: value :: HNil
        )
      } catch {
        case e: Throwable => -\/(GetError(settings.binName, e))
      }
    }
  }

  def all[R](settings: Settings)(
    implicit
    decoder: Decoder[R]
  ) = withClient { client =>
    Task.async[Option[R]] { register =>
      try {
        execute[Seq[String], Map[String, R]](
          client.cluster,
          client.policy.asyncWritePolicyDefault,
          Some(new ExecuteListener[Map[String, R]] {
            override def onFailure(e: AerospikeException): Unit = register(-\/(e))

            override def onSuccess(key: Key, a: Option[Record[Map[String, R]]]): Unit = {
              val o = a.collect {
                case Record(Some(x), _, _) => x
              }
              val x = o getOrElse Map.empty
              register(\/-(x.get("SUCCESS")))
            }
          }),
          Key(settings.namespace, settings.setName, settings.key),
          PackageName,
          "scan",
          settings.binName :: Nil
        )
      } catch {
        case e: Throwable => -\/(GetError(settings.binName, e))
      }
    }
  }

  def delete(settings: Settings, name: String)(
    implicit
    encoder: Encoder[String :: String :: HNil]
  ) = withClient { client =>
    Task.async[Unit] { register =>
      try {
        execute[String :: String :: HNil, Map[String, Int]](
          client.cluster,
          client.policy.asyncWritePolicyDefault,
          Some(new ExecuteListener[Map[String, Int]] {
            override def onFailure(e: AerospikeException): Unit = register(-\/(e))

            override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
              val o = a.collect {
                case Record(Some(x), _, _) => x
              }
              val x = o getOrElse Map.empty
              if (x.contains("SUCCESS"))
                register(\/-({}))
              else if (x.contains("FAILURE"))
                register(-\/(new AerospikeException(a.toString)))
              else
                register(-\/(new AerospikeException("Invalid UDF return value")))
            }
          }),
          Key(settings.namespace, settings.setName, settings.key),
          PackageName,
          "remove",
          settings.binName :: name :: HNil
        )
      } catch {
        case e: Throwable => -\/(DeleteError(settings.binName, e))
      }
    }
  }

  def deleteBin(settings: Settings)(
    implicit
    encoder: Encoder[String :: HNil]
  ) = withClient { client =>
    Task.async[Unit] { register =>
      try {
        execute[String :: HNil, Map[String, Int]](
          client.cluster,
          client.policy.asyncWritePolicyDefault,
          Some(new ExecuteListener[Map[String, Int]] {
            override def onFailure(e: AerospikeException): Unit = register(-\/(e))

            override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
              val o = a.collect {
                case Record(Some(x), _, _) => x
              }
              val x = o getOrElse Map.empty
              if (x.contains("SUCCESS"))
                register(\/-({}))
              else if (x.contains("FAILURE"))
                register(-\/(new AerospikeException(a.toString)))
              else
                register(-\/(new AerospikeException("Invalid UDF return value")))
            }
          }),
          Key(settings.namespace, settings.setName, settings.key),
          PackageName,
          "destroy",
          settings.binName :: HNil
        )
      } catch {
        case e: Throwable => -\/(DeleteError(settings.binName, e))
      }
    }
  }
}
