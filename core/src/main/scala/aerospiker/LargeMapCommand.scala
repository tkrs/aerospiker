package aerospiker

import aerospiker.command._
import aerospiker.listener._
import aerospiker.policy._
import com.aerospike.client.AerospikeException
import com.aerospike.client.async.AsyncCluster
import io.circe.{ Encoder, Decoder }
import scalaz.{ \/, \/-, -\/ }

object LargeMapCommand {

  private[this] val PackageName = "lmap"

  private[this] def execute[A, R](
    cluster: AsyncCluster,
    policy: WritePolicy,
    listener: Option[ExecuteListener[R]],
    key: Key,
    packageName: String,
    functionName: String,
    functionArgs: A
  )(
    implicit
    encoder: Encoder[A],
    decoder: Decoder[R]
  ) =
    new UdfExecute[A, R](cluster, policy, listener, key, packageName, functionName, functionArgs).execute()

  def get[R](client: AerospikeClient, settings: Settings, a: String, cb: Throwable \/ Option[R] => Unit)(implicit decoder: Decoder[R]) =
    try {
      execute[Seq[String], Map[String, Map[String, R]]](
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
        Seq(settings.binName, a)
      )
    } catch {
      case e: Throwable => -\/(GetError(settings.binName, e))
    }

  def puts[A](client: AerospikeClient, settings: Settings, m: A, cb: Throwable \/ Unit => Unit)(implicit encoder: Encoder[(String, A)]) =
    try {
      execute[(String, A), Map[String, Int]](
        client.cluster,
        client.policy.asyncWritePolicyDefault,
        Some(new ExecuteListener[Map[String, Int]] {
          override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
          override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
            val o = a.collect { case Record(Some(x), _, _) => x }
            val x = o getOrElse Map.empty
            if (x.contains("SUCCESS")) cb(\/-({}))
            else if (x.contains("FAILURE")) cb(-\/(new AerospikeException(a.toString)))
            else cb(-\/(new AerospikeException("Invalid UDF return value")))
          }
        }),
        Key(settings.namespace, settings.setName, settings.key),
        PackageName,
        "put_all",
        (settings.binName, m)
      )
    } catch {
      case e: Throwable => cb(-\/(PutError(settings.binName, e)))
    }

  def put[A](client: AerospikeClient, settings: Settings, name: String, value: A, cb: Throwable \/ Unit => Unit)(implicit encoder: Encoder[(String, String, A)]) =
    try {
      execute[(String, String, A), Map[String, Int]](
        client.cluster,
        client.policy.asyncWritePolicyDefault,
        Some(new ExecuteListener[Map[String, Int]] {
          override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
          override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
            val o = a.collect { case Record(Some(x), _, _) => x }
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
        (settings.binName, name, value)
      )
    } catch {
      case e: Throwable => -\/(GetError(settings.binName, e))
    }

  def all[R](client: AerospikeClient, settings: Settings, cb: Throwable \/ Option[R] => Unit)(implicit decoder: Decoder[R]) =
    try {
      execute[Seq[String], Map[String, R]](
        client.cluster,
        client.policy.asyncWritePolicyDefault,
        Some(new ExecuteListener[Map[String, R]] {
          override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
          override def onSuccess(key: Key, a: Option[Record[Map[String, R]]]): Unit = {
            val o = a.collect { case Record(Some(x), _, _) => x }
            val x = o getOrElse Map.empty
            cb(\/-(x.get("SUCCESS")))
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

  def delete(client: AerospikeClient, settings: Settings, name: String, cb: Throwable \/ Unit => Unit)(implicit encoder: Encoder[(String, String)]) =
    try {
      println("delete:" + settings)
      execute[(String, String), Map[String, Int]](
        client.cluster,
        client.policy.asyncWritePolicyDefault,
        Some(new ExecuteListener[Map[String, Int]] {
          override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
          override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
            val o = a.collect { case Record(Some(x), _, _) => x }
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
        "remove",
        (settings.binName, name)
      )
    } catch {
      case e: Throwable => -\/(DeleteError(settings.binName, e))
    }

  def deleteBin(client: AerospikeClient, settings: Settings, cb: Throwable \/ Unit => Unit)(implicit encoder: Encoder[String]) =
    try {
      println("deleteBin:" + settings)
      execute[Seq[String], Map[String, Int]](
        client.cluster,
        client.policy.asyncWritePolicyDefault,
        Some(new ExecuteListener[Map[String, Int]] {
          override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
          override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
            val o = a.collect { case Record(Some(x), _, _) => x }
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
        "destroy",
        Seq(settings.binName)
      )
    } catch {
      case e: Throwable => -\/(DeleteError(settings.binName, e))
    }

}
