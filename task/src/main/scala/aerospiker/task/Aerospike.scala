package aerospiker
package task

import java.util.concurrent.CountDownLatch

import aerospiker.data.Record
import aerospiker.listener._
import aerospiker.command._
import com.aerospike.client.{ Operation, AerospikeException }
import io.circe.{ Encoder, Decoder }
import scala.collection.mutable.ListBuffer
import scalaz.concurrent.Task
import scalaz.{ \/, -\/, \/- }

object Aerospike {

  import Command._

  def get[U](settings: Settings, binNames: String*)(implicit decoder: Decoder[U]): Action[U] = withClient[U] { client =>
    Task.async { cb =>
      new Read(
        client.cluster,
        client.policy.asyncReadPolicyDefault,
        Some(new RecordListener[U] {
          override def onFailure(e: AerospikeException): Unit = {
            cb(-\/(GetError(settings.key, e)))
          }
          override def onSuccess(key: Key, record: Option[Record[U]]): Unit = {
            record match {
              case None => cb(-\/(NoSuchKey(settings.key)))
              case Some(r) => r.bins match {
                case None => cb(-\/(GetError(settings.key)))
                case Some(bins) => cb(\/-(bins))
              }
            }
          }
        }),
        Key(settings.namespace, settings.setName, settings.key),
        binNames: _*
      ).execute()
    }
  }

  private[this] def putExec[U](settings: Settings, bins: U, client: AerospikeClient)(implicit encoder: Encoder[U]): Task[Unit] =
    Task.async[Unit] { cb =>
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
    }

  def put[U](settings: Settings, bins: U)(implicit encoder: Encoder[U]) = withClient[Unit] { client =>
    putExec(settings, bins, client)
  }

  def puts[U](settings: Settings, kvs: Map[String, U])(implicit encoder: Encoder[U]) = withClient { client =>
    Task.delay {
      val latch = new CountDownLatch(kvs.size)
      val list = ListBuffer.empty[Throwable \/ String]
      kvs foreach {
        case (k, v) => putExec(settings.copy(key = k), v, client) runAsync { r =>
          list += r.map(_ => k)
          latch.countDown()
        }
      }
      latch.await()
      list.toSeq
    }
  }

  private[this] def deleteExec(settings: Settings, client: AerospikeClient) = Task.async[Boolean] { cb =>
    new Delete(
      client.cluster,
      client.policy.asyncWritePolicyDefault,
      Some(new DeleteListener {
        override def onFailure(e: AerospikeException): Unit = cb(-\/(DeleteError(settings.key, e)))
        override def onSuccess(key: Key, exists: Boolean): Unit = cb(\/-(exists))
      }),
      Key(settings.namespace, settings.setName, settings.key)
    ).execute()
  }

  def delete(settings: Settings) = withClient[Boolean] { client =>
    deleteExec(settings, client)
  }

  def deletes(settings: Settings, keys: Seq[String]) = withClient { client =>
    Task.delay {
      val latch = new CountDownLatch(keys.size)
      val list = ListBuffer.empty[Throwable \/ String]
      keys foreach { k =>
        deleteExec(settings.copy(key = k), client) runAsync { r =>
          list += r.map(_ => k)
          latch.countDown()
        }
      }
      latch.await()
      list.toSeq
    }
  }

  def all[A](settings: Settings, binNames: String*)(
    implicit
    decoder: Decoder[A]
  ) = withClient { client =>
    Task.async[Seq[(Key, Option[Record[A]])]] { cb =>
      import scala.collection.mutable.ListBuffer
      val buffer: ListBuffer[(Key, Option[Record[A]])] = ListBuffer.empty
      new ScanExecutor(
        client.cluster,
        client.policy.asyncScanPolicyDefault,
        Some(new RecordSequenceListener[A] {
          def onRecord(key: Key, record: Option[Record[A]]): Unit = buffer += key -> record
          def onFailure(e: AerospikeException): Unit = cb(-\/(e))
          def onSuccess(): Unit = cb(\/-(buffer.toSeq))
        }),
        settings.namespace,
        settings.setName,
        binNames.toArray
      ).execute()
    }
  }

  def exists(settings: Settings) = withClient { client =>
    Task.async[Boolean] { register =>
      new Exists(
        client.cluster,
        client.policy.asyncReadPolicyDefault,
        Some(new ExistsListener {
          def onSuccess(key: Key, exists: Boolean): Unit = { register(\/-(exists)) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        }),
        Key(settings.namespace, settings.setName, settings.key)
      ).execute()
    }
  }

}

