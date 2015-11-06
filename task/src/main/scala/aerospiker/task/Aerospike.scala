package aerospiker
package task

import io.circe.{ Encoder, Decoder }
import scalaz.{ \/, Nondeterminism }
import scalaz.concurrent.Task

object Aerospike extends Functions {

  type C = AerospikeClient

  def get[U](settings: Settings, binNames: String*)(implicit decoder: Decoder[U]) =
    withC[Task, U] { c =>
      Task.fork {
        Task.async[U] { cb =>
          Command.get[U](c, settings, binNames, cb)
        }
      }
    }

  private[this] def putExec[U](c: C, settings: Settings, bins: U)(implicit encoder: Encoder[U]) =
    Task.fork {
      Task.async[Unit] { cb =>
        Command.put(c, settings, bins, cb)
      }
    }

  def put[U](settings: Settings, bins: U)(implicit encoder: Encoder[U]) =
    withC[Task, Unit] { c =>
      putExec(c, settings, bins)
    }

  def puts[U](settings: Settings, kv: Map[String, U])(implicit encoder: Encoder[U]) =
    withC[Task, Seq[Throwable \/ String]] { c =>
      Task.fork {
        implicitly[Nondeterminism[Task]].gather {
          kv map {
            case (k, v) => putExec(c, settings.copy(key = k), v).map(_ => k).attempt
          } toSeq
        }
      }
    }

  private[this] def deleteExec(c: C, settings: Settings) =
    Task.fork {
      Task.async[Boolean] { cb =>
        Command.delete(c, settings, cb)
      }
    }

  def delete(settings: Settings) =
    withC[Task, Boolean] { c =>
      deleteExec(c, settings)
    }

  def deletes(settings: Settings, keys: Seq[String]) =
    withC[Task, Seq[Throwable \/ String]] { c =>
      Task.fork {
        implicitly[Nondeterminism[Task]].gather {
          keys.map { k =>
            deleteExec(c, settings.copy(key = k)).map(_ => k).attempt
          } toSeq
        }
      }
    }

  def all[A](settings: Settings, binNames: String*)(implicit decoder: Decoder[A]) =
    withC[Task, Seq[(Key, Option[Record[A]])]] { c =>
      Task.fork {
        Task.async[Seq[(Key, Option[Record[A]])]] { cb =>
          Command.all[A](c, settings, binNames, cb)
        }
      }
    }

  def exists(settings: Settings) =
    withC[Task, Boolean] { c =>
      Task.fork {
        Task.async[Boolean] { cb =>
          Command.exists(c, settings, cb)
        }
      }
    }
}
