package aerospiker

import aerospiker.listener._
import aerospiker.command._
import com.aerospike.client.Operation
import io.circe.{ Encoder, Decoder }

object Command {

  def get[U: Decoder](client: AerospikeClient, settings: Settings, key: String, binNames: Seq[String], listener: Option[RecordListener[U]]): Unit =
    new Read(client.cluster, client.policy.asyncReadPolicyDefault, listener, Key(settings.namespace, settings.setName, key), binNames: _*).execute()

  def put[U: Encoder](client: AerospikeClient, settings: Settings, key: String, bins: U, listener: Option[WriteListener]): Unit =
    new Write[U](client.cluster, client.policy.asyncWritePolicyDefault, listener, Key(settings.namespace, settings.setName, key), bins, Operation.Type.WRITE).execute()

  def delete(client: AerospikeClient, settings: Settings, key: String, listener: Option[DeleteListener]): Unit =
    new Delete(client.cluster, client.policy.asyncWritePolicyDefault, listener, Key(settings.namespace, settings.setName, key)).execute()

  def all[A: Decoder](client: AerospikeClient, settings: Settings, binNames: Seq[String], listener: Option[RecordSequenceListener[A]]): Unit = {
    new ScanExecutor(client.cluster, client.policy.asyncScanPolicyDefault, listener, settings.namespace, settings.setName, binNames.toArray).execute()
  }

  def exists(client: AerospikeClient, settings: Settings, key: String, listener: Option[ExistsListener]): Unit =
    new Exists(client.cluster, client.policy.asyncReadPolicyDefault, listener, Key(settings.namespace, settings.setName, key)).execute()
}
