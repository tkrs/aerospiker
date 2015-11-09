package aerospiker
package task

import java.util.concurrent.ConcurrentLinkedQueue
import com.aerospike.client.AerospikeException

import aerospiker.listener._
import io.circe.{ Encoder, Decoder }
import scalaz.{ \/, \/-, -\/, Nondeterminism }
import scalaz.concurrent.Task

import scala.collection.JavaConversions._

object Aerospike extends Functions {

  type C = AerospikeClient

  def get[U](settings: Settings, binNames: String*)(implicit decoder: Decoder[U]) =
    withC[Task, U] { c =>
      Task.fork {
        Task.async[U] { cb =>
          try {
            Command.get[U](c, settings, binNames,
              Some(new RecordListener[U] {
                override def onFailure(e: AerospikeException): Unit = cb(-\/(GetError(settings.key, e)))
                override def onSuccess(key: Key, record: Option[Record[U]]): Unit = record match {
                  case None => cb(-\/(NoSuchKey(settings.key)))
                  case Some(r) => r.bins match {
                    case None => cb(-\/(GetError(settings.key)))
                    case Some(bins) => cb(\/-(bins))
                  }
                }
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }

  def put[U](settings: Settings, bins: U)(implicit encoder: Encoder[U]) =
    withC[Task, Unit] { c =>
      Task.fork {
        Task.async[Unit] { cb =>
          try {
            Command.put(c, settings, bins,
              Some(new WriteListener {
                override def onFailure(e: AerospikeException): Unit = cb(-\/(PutError(settings.key, e)))
                override def onSuccess(key: Key): Unit = cb(\/-({}))
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }

  def puts[U](settings: Settings, kv: Map[String, U])(implicit encoder: Encoder[U]) =
    withC[Task, Seq[Throwable \/ String]] { c =>
      Task.fork {
        implicitly[Nondeterminism[Task]].gather {
          kv map {
            case (k, v) =>
              put(settings.copy(key = k), v).run(c).map(_ => k).attempt
          } toSeq
        }
      }
    }

  def delete(settings: Settings) =
    withC[Task, Boolean] { c =>
      Task.fork {
        Task.async[Boolean] { cb =>
          try {
            Command.delete(c, settings,
              Some(new DeleteListener {
                override def onFailure(e: AerospikeException): Unit = cb(-\/(DeleteError(settings.key, e)))
                override def onSuccess(key: Key, exists: Boolean): Unit = cb(\/-(exists))
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }

  def deletes(settings: Settings, keys: Seq[String]) =
    withC[Task, Seq[Throwable \/ String]] { c =>
      Task.fork {
        implicitly[Nondeterminism[Task]].gather {
          keys.map { k =>
            delete(settings.copy(key = k)).run(c).map(_ => k).attempt
          } toSeq
        }
      }
    }

  def all[A](settings: Settings, binNames: String*)(implicit decoder: Decoder[A]) =
    withC[Task, Seq[(Key, Option[Record[A]])]] { c =>
      Task.fork {
        Task.async[Seq[(Key, Option[Record[A]])]] { cb =>
          try {
            val queue = new ConcurrentLinkedQueue[(Key, Option[Record[A]])]
            Command.all[A](c, settings, binNames,
              Some(new RecordSequenceListener[A] {
                def onRecord(key: Key, record: Option[Record[A]]): Unit = queue.add(key -> record)
                def onFailure(e: AerospikeException): Unit = cb(-\/(e))
                def onSuccess(): Unit = cb(\/-(queue.toSeq))
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }

  def exists(settings: Settings) =
    withC[Task, Boolean] { c =>
      Task.fork {
        Task.async[Boolean] { cb =>
          try {
            Command.exists(c, settings,
              Some(new ExistsListener {
                def onSuccess(key: Key, exists: Boolean): Unit = cb(\/-(exists))
                def onFailure(e: AerospikeException): Unit = cb(-\/(e))
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }
}
