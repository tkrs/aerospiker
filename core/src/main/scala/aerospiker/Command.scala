package aerospiker

import java.util.concurrent.ConcurrentLinkedQueue

import aerospiker.listener._
import aerospiker.command._
import com.aerospike.client.{ Operation, AerospikeException }
import io.circe.{ Encoder, Decoder }
import scalaz.concurrent.Task
import scalaz.{ \/, -\/, \/- }

import scala.collection.JavaConversions._

object Command {

  def get[U](client: AerospikeClient, settings: Settings, binNames: Seq[String], cb: Throwable \/ U => Unit)(implicit decoder: Decoder[U]) =
    try {
      new Read(
        client.cluster,
        client.policy.asyncReadPolicyDefault,
        Some(new RecordListener[U] {
          override def onFailure(e: AerospikeException): Unit = cb(-\/(GetError(settings.key, e)))
          override def onSuccess(key: Key, record: Option[Record[U]]): Unit = record match {
            case None => cb(-\/(NoSuchKey(settings.key)))
            case Some(r) => r.bins match {
              case None => cb(-\/(GetError(settings.key)))
              case Some(bins) => cb(\/-(bins))
            }
          }
        }),
        Key(settings.namespace, settings.setName, settings.key),
        binNames: _*
      ).execute()
    } catch {
      case e: Throwable => cb(-\/(GetError(settings.key, e)))
    }

  def put[U](client: AerospikeClient, settings: Settings, bins: U, cb: Throwable \/ Unit => Unit)(implicit encoder: Encoder[U]) =
    try {
      new Write[U](
        client.cluster,
        client.policy.asyncWritePolicyDefault,
        Some(new WriteListener {
          override def onFailure(e: AerospikeException): Unit = cb(-\/(PutError(settings.key, e)))
          override def onSuccess(key: Key): Unit = cb(\/-({}))
        }),
        Key(settings.namespace, settings.setName, settings.key),
        bins,
        Operation.Type.WRITE
      ).execute()
    } catch {
      case e: Throwable => -\/(PutError(settings.key, e))
    }

  def delete(client: AerospikeClient, settings: Settings, cb: Throwable \/ Boolean => Unit) =
    try {
      new Delete(
        client.cluster,
        client.policy.asyncWritePolicyDefault,
        Some(new DeleteListener {
          override def onFailure(e: AerospikeException): Unit = cb(-\/(DeleteError(settings.key, e)))
          override def onSuccess(key: Key, exists: Boolean): Unit = cb(\/-(exists))
        }),
        Key(settings.namespace, settings.setName, settings.key)
      ).execute()
    } catch {
      case e: Throwable => -\/(DeleteError(settings.key, e))
    }

  def all[A](client: AerospikeClient, settings: Settings, binNames: Seq[String], cb: Throwable \/ Seq[(Key, Option[Record[A]])] => Unit)(implicit decoder: Decoder[A]) =
    try {
      val queue = new ConcurrentLinkedQueue[(Key, Option[Record[A]])]
      new ScanExecutor(
        client.cluster,
        client.policy.asyncScanPolicyDefault,
        Some(new RecordSequenceListener[A] {
          def onRecord(key: Key, record: Option[Record[A]]): Unit = queue.add(key -> record)
          def onFailure(e: AerospikeException): Unit = cb(-\/(e))
          def onSuccess(): Unit = cb(\/-(queue.toSeq))
        }),
        settings.namespace,
        settings.setName,
        binNames.toArray
      ).execute()
    } catch {
      case e: Throwable => -\/(GetError(settings.key, e))
    }

  def exists(client: AerospikeClient, settings: Settings, cb: Throwable \/ Boolean => Unit) =
    try {
      new Exists(
        client.cluster,
        client.policy.asyncReadPolicyDefault,
        Some(new ExistsListener {
          def onSuccess(key: Key, exists: Boolean): Unit = cb(\/-(exists))
          def onFailure(e: AerospikeException): Unit = cb(-\/(e))
        }),
        Key(settings.namespace, settings.setName, settings.key)
      ).execute()
    } catch {
      case e: Throwable => -\/(GetError(settings.key, e))
    }

}
