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

  def get[R](client: AerospikeClient, settings: Settings, a: String, listener: Option[ExecuteListener[Map[String, Map[String, R]]]])(implicit decoder: Decoder[R]) =
    execute[Seq[String], Map[String, Map[String, R]]](
      client.cluster,
      client.policy.asyncWritePolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key),
      PackageName,
      "get",
      Seq(settings.binName, a)
    )

  def puts[A](client: AerospikeClient, settings: Settings, m: A, listener: Option[ExecuteListener[Map[String, Int]]])(implicit encoder: Encoder[(String, A)]) =
    execute[(String, A), Map[String, Int]](
      client.cluster,
      client.policy.asyncWritePolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key),
      PackageName,
      "put_all",
      (settings.binName, m)
    )

  def put[A](client: AerospikeClient, settings: Settings, name: String, value: A, listener: Option[ExecuteListener[Map[String, Int]]])(implicit encoder: Encoder[(String, String, A)]) =
    execute[(String, String, A), Map[String, Int]](
      client.cluster,
      client.policy.asyncWritePolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key),
      PackageName,
      "put",
      (settings.binName, name, value)
    )

  def all[R](client: AerospikeClient, settings: Settings, listener: Option[ExecuteListener[Map[String, R]]])(implicit decoder: Decoder[R]) =
    execute[Seq[String], Map[String, R]](
      client.cluster,
      client.policy.asyncWritePolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key),
      PackageName,
      "scan",
      settings.binName :: Nil
    )

  def delete(client: AerospikeClient, settings: Settings, name: String, listener: Option[ExecuteListener[Map[String, Int]]])(implicit encoder: Encoder[(String, String)]) =
    execute[(String, String), Map[String, Int]](
      client.cluster,
      client.policy.asyncWritePolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key),
      PackageName,
      "remove",
      (settings.binName, name)
    )

  def deleteBin(client: AerospikeClient, settings: Settings, listener: Option[ExecuteListener[Map[String, Int]]])(implicit encoder: Encoder[String]) =
    execute[Seq[String], Map[String, Int]](
      client.cluster,
      client.policy.asyncWritePolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key),
      PackageName,
      "destroy",
      Seq(settings.binName)
    )

}
