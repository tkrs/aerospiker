package aerospiker
package task

import aerospiker.{ Record, LargeMapCommand => Command }
import aerospiker.command._
import aerospiker.listener._
import aerospiker.policy._
import com.aerospike.client.AerospikeException
import com.aerospike.client.async.AsyncCluster
import io.circe.{ Encoder, Decoder }

import scalaz.{ \/-, -\/ }
import scalaz.concurrent.Task

object AerospikeLargeMap extends Functions {

  def get[R](settings: Settings, a: String)(implicit decoder: Decoder[R]) =
    withClient[Task, Option[R]] { client =>
      implicit val dec = Decoder[Map[String, Map[String, R]]]
      implicit val enc = Encoder[Seq[String]]
      Task.async[Option[R]] { cb =>
        Command.get(client, settings, a, cb)
      }
    }

  def puts[A](settings: Settings, m: A)(implicit encoder: Encoder[(String, A)]) =
    withClient[Task, Unit] { client =>
      Task.async[Unit] { cb =>
        Command.puts(client, settings, m, cb)
      }
    }

  def put[A](settings: Settings, name: String, value: A)(implicit encoder: Encoder[(String, String, A)]) =
    withClient[Task, Unit] { client =>
      Task.async[Unit] { cb =>
        Command.put(client, settings, name, value, cb)
      }
    }

  def all[R](settings: Settings)(implicit decoder: Decoder[R]) =
    withClient[Task, Option[R]] { client =>
      Task.async[Option[R]] { cb =>
        Command.all(client, settings, cb)
      }
    }

  def delete(settings: Settings, name: String)(implicit encoder: Encoder[(String, String)]) =
    withClient[Task, Unit] { client =>
      Task.async[Unit] { cb =>
        Command.delete(client, settings, name, cb)
      }
    }

  def deleteBin(settings: Settings)(implicit encoder: Encoder[String]) =
    withClient[Task, Unit] { client =>
      Task.async[Unit] { cb =>
        Command.deleteBin(client, settings, cb)
      }
    }
}
