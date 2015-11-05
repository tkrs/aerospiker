package aerospiker
package task

import aerospiker.{ AerospikeClient, Record, Settings }
import aerospiker.command._
import aerospiker.listener._
import com.aerospike.client.{ Operation, AerospikeException }
import io.circe.{ Encoder, Decoder }
import scalaz.{ \/, -\/, \/-, Nondeterminism }
import scalaz.concurrent.Task

import scala.collection.JavaConversions._

object Aerospike extends Functions {

  def get[U](settings: Settings, binNames: String*)(implicit decoder: Decoder[U]) =
    withClient[Task, U] { client =>
      Task.fork(Task.async[U](cb => Command.get[U](client, settings, binNames, cb)))
    }

  private[this] def putExec[U](client: AerospikeClient, settings: Settings, bins: U)(implicit encoder: Encoder[U]) =
    Task.fork(Task.async[Unit](cb => Command.put(client, settings, bins, cb)))

  def put[U](settings: Settings, bins: U)(implicit encoder: Encoder[U]) =
    withClient[Task, Unit] { client =>
      putExec(client, settings, bins)
    }

  def puts[U](settings: Settings, kvs: Map[String, U])(implicit encoder: Encoder[U]) =
    withClient[Task, Seq[Throwable \/ String]] { client =>
      Task.fork {
        implicitly[Nondeterminism[Task]].gather {
          kvs map {
            case (k, v) =>
              putExec(client, settings.copy(key = k), v).map(_ => k).attempt
          } toSeq
        }
      }
    }

  private[this] def deleteExec(client: AerospikeClient, settings: Settings) =
    Task.fork {
      Task.async[Boolean] { cb =>
        Command.delete(client, settings, cb)
      }
    }

  def delete(settings: Settings) =
    withClient[Task, Boolean] { client =>
      deleteExec(client, settings)
    }

  def deletes(settings: Settings, keys: Seq[String]) =
    withClient[Task, Seq[Throwable \/ String]] { client =>
      Task.fork {
        implicitly[Nondeterminism[Task]].gather(keys.map(k => deleteExec(client, settings.copy(key = k)).map(_ => k).attempt).toSeq)
      }
    }

  def all[A](settings: Settings, binNames: String*)(implicit decoder: Decoder[A]) =
    withClient[Task, Seq[(Key, Option[Record[A]])]] { client =>
      Task.fork {
        Task.async[Seq[(Key, Option[Record[A]])]] { cb =>
          Command.all[A](client, settings, binNames, cb)
        }
      }
    }

  def exists(settings: Settings) =
    withClient[Task, Boolean] { client =>
      Task.fork {
        Task.async[Boolean] { register =>
          ???
        }
      }
    }
}
