package aerospiker
package task

import aerospiker.listener._
import com.aerospike.client.AerospikeException
import io.circe.{ Decoder, Encoder }
import monix.eval.Task
import monix.execution.Cancelable

import scala.collection.generic.CanBuildFrom

object Aerospike extends Functions {

  def get[U: Decoder](settings: Settings, binNames: String*): Action[Task, U] =
    Action[Task, U] { c =>
      Task.async[U] { (s, cb) =>
        try {
          Command.get[U](c, settings, binNames,
            Some(new RecordListener[U] {
              override def onFailure(e: AerospikeException): Unit = cb.onError(GetError(settings.key, e))
              override def onSuccess(key: Key, record: Option[Record[U]]): Unit = record match {
                case None => cb.onError(NoSuchKey(settings.key))
                case Some(r) => r.bins match {
                  case None => cb.onError(GetError(settings.key))
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

  def put[U: Encoder](settings: Settings, bins: U): Action[Task, Unit] =
    Action[Task, Unit] { c =>
      Task.async[Unit] { (s, cb) =>
        try {
          Command.put(c, settings, bins,
            Some(new WriteListener {
              override def onFailure(e: AerospikeException): Unit = cb.onError(PutError(settings.key, e))
              override def onSuccess(key: Key): Unit = cb.onSuccess(())
            }))
        } catch {
          case e: Throwable => cb.onError(e)
        }
        Cancelable.empty
      }
    }

  //  def puts[U](settings: Settings, kv: Map[String, U])(implicit encoder: Encoder[U]): Action[Task, Seq[String]] =
  //    kv.toList.traverse { case (k: String, v: U) => put(settings.copy(key = k), v).map(_ => k) }

  def delete(settings: Settings): Action[Task, Boolean] =
    Action[Task, Boolean] { c =>
      Task.async[Boolean] { (s, cb) =>
        try {
          Command.delete(c, settings,
            Some(new DeleteListener {
              override def onFailure(e: AerospikeException): Unit = cb.onError(DeleteError(settings.key, e))
              override def onSuccess(key: Key, exists: Boolean): Unit = cb.onSuccess(exists)
            }))
        } catch {
          case e: Throwable => cb.onError(e)
        }
        Cancelable.empty
      }
    }

  //  def deletes(settings: Settings, keys: Seq[String]): Action[Task, Seq[String]] =
  //    keys.toList.traverse(k => delete(settings.copy(key = k)).map(_ => k))

  def all[C[_], A](settings: Settings, binNames: String*)(
    implicit
    decoder: Decoder[A],
    cbf: CanBuildFrom[Nothing, (Key, Option[Record[A]]), C[(Key, Option[Record[A]])]]
  ): Action[Task, C[(Key, Option[Record[A]])]] =
    Action[Task, C[(Key, Option[Record[A]])]] { c =>
      Task.async[C[(Key, Option[Record[A]])]] { (s, cb) =>
        try {
          val builder = cbf.apply()
          Command.all[A](c, settings, binNames,
            Some(new RecordSequenceListener[A] {
              def onRecord(key: Key, record: Option[Record[A]]): Unit = builder += key -> record
              def onFailure(e: AerospikeException): Unit = cb.onError(e)
              def onSuccess(): Unit = cb.onSuccess(builder.result())
            }))
        } catch {
          case e: Throwable => cb.onError(e)
        }
        Cancelable.empty
      }
    }

  def exists(settings: Settings): Action[Task, Boolean] =
    Action[Task, Boolean] { c =>
      Task.async[Boolean] { (s, cb) =>
        try {
          Command.exists(c, settings,
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
