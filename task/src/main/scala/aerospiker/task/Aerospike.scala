package aerospiker
package task

import java.util.concurrent.ConcurrentLinkedQueue

import aerospiker.listener._
import com.aerospike.client.AerospikeException
import io.circe.{ Decoder, Encoder }
import monix.eval.Task
import monix.execution.Cancelable
import monix.cats._

import scala.collection.JavaConverters._

object Aerospike extends Functions {

  type E[A] = Action[Task, A]

  def get[U: Decoder](settings: Settings, key: String, binNames: String*): Action[Task, U] =
    Action[Task, U] { c =>
      Task.async[U] { (s, cb) =>
        try {
          Command.get[U](c, settings, key, binNames,
            Some(new RecordListener[U] {
              override def onFailure(e: AerospikeException): Unit = cb.onError(GetError(key, e))
              override def onSuccess(_key: Key, record: Option[Record[U]]): Unit = record match {
                case None => cb.onError(NoSuchKey(key))
                case Some(r) => r.bins match {
                  case None => cb.onError(GetError(key))
                  case Some(bins) => cb.onSuccess(bins)
                }
              }
            }))
        } catch {
          case e: Throwable => cb.onError(e)
        }
        Cancelable.empty
      }
    }

  def put[U: Encoder](settings: Settings, key: String, bins: U): Action[Task, Unit] =
    Action[Task, Unit] { c =>
      Task.async[Unit] { (s, cb) =>
        try {
          Command.put(c, settings, key, bins,
            Some(new WriteListener {
              override def onFailure(e: AerospikeException): Unit = cb.onError(PutError(key, e))
              override def onSuccess(key: Key): Unit = cb.onSuccess(())
            }))
        } catch {
          case e: Throwable => cb.onError(e)
        }
        Cancelable.empty
      }
    }

  def delete(settings: Settings, key: String): Action[Task, Boolean] =
    Action[Task, Boolean] { c =>
      Task.async[Boolean] { (s, cb) =>
        try {
          Command.delete(c, settings, key,
            Some(new DeleteListener {
              override def onFailure(e: AerospikeException): Unit = cb.onError(DeleteError(key, e))
              override def onSuccess(key: Key, exists: Boolean): Unit = cb.onSuccess(exists)
            }))
        } catch {
          case e: Throwable => cb.onError(e)
        }
        Cancelable.empty
      }
    }

  def all[A](settings: Settings, binNames: String*)(implicit decoder: Decoder[A]): Action[Task, Seq[(Key, Option[Record[A]])]] =
    Action[Task, Seq[(Key, Option[Record[A]])]] { c =>
      Task.async[Seq[(Key, Option[Record[A]])]] { (s, cb) =>
        try {
          val queue = new ConcurrentLinkedQueue[(Key, Option[Record[A]])]
          Command.all[A](c, settings, binNames,
            Some(new RecordSequenceListener[A] {
              def onRecord(key: Key, record: Option[Record[A]]): Unit = queue.add(key -> record)
              def onFailure(e: AerospikeException): Unit = cb.onError(e)
              def onSuccess(): Unit = cb.onSuccess(queue.asScala.toSeq)
            }))
        } catch {
          case e: Throwable => cb.onError(e)
        }
        Cancelable.empty
      }
    }

  def exists(settings: Settings, key: String): Action[Task, Boolean] =
    Action[Task, Boolean] { c =>
      Task.async[Boolean] { (s, cb) =>
        try {
          Command.exists(c, settings, key,
            Some(new ExistsListener {
              def onSuccess(key: Key, exists: Boolean): Unit = cb.onSuccess(exists)
              def onFailure(e: AerospikeException): Unit = cb.onError(e)
            }))
        } catch {
          case e: Throwable => cb.onError(e)
        }
        Cancelable.empty
      }
    }
}
