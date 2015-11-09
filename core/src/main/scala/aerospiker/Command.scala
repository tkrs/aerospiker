package aerospiker

import aerospiker.listener._
import aerospiker.command._
import com.aerospike.client.{ Operation, AerospikeException }
import io.circe.{ Encoder, Decoder }
import scalaz.{ \/, -\/, \/- }

object Command {

  def get[U](client: AerospikeClient, settings: Settings, binNames: Seq[String], listener: Option[RecordListener[U]])(implicit decoder: Decoder[U]) =
    new Read(
      client.cluster,
      client.policy.asyncReadPolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key),
      binNames: _*
    ).execute()

  def put[U](client: AerospikeClient, settings: Settings, bins: U, listener: Option[WriteListener])(implicit encoder: Encoder[U]) =
    new Write[U](
      client.cluster,
      client.policy.asyncWritePolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key),
      bins,
      Operation.Type.WRITE
    ).execute()

  def delete(client: AerospikeClient, settings: Settings, listener: Option[DeleteListener]) =
    new Delete(
      client.cluster,
      client.policy.asyncWritePolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key)
    ).execute()

  def all[A](client: AerospikeClient, settings: Settings, binNames: Seq[String], listener: Option[RecordSequenceListener[A]])(implicit decoder: Decoder[A]) = {
    new ScanExecutor(
      client.cluster,
      client.policy.asyncScanPolicyDefault,
      listener,
      settings.namespace,
      settings.setName,
      binNames.toArray
    ).execute()
  }

  def exists(client: AerospikeClient, settings: Settings, listener: Option[ExistsListener]) =
    new Exists(
      client.cluster,
      client.policy.asyncReadPolicyDefault,
      listener,
      Key(settings.namespace, settings.setName, settings.key)
    ).execute()
}
