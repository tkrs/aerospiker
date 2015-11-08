package aerospiker
package task

import aerospiker.{ LargeMapCommand => Command }
import io.circe.{ Encoder, Decoder }

import scalaz.concurrent.Task

object AerospikeLargeMap extends Functions {

  type C = AerospikeClient

  def get[R](settings: Settings, a: String)(implicit decoder: Decoder[R]) =
    withC[Task, Option[R]] { c =>
      implicit val dec = Decoder[Map[String, Map[String, R]]]
      implicit val enc = Encoder[Seq[String]]
      Task.fork {
        Task.async[Option[R]] { cb =>
          Command.get(c, settings, a, cb)
        }
      }
    }

  def puts[A](settings: Settings, m: A)(implicit encoder: Encoder[(String, A)]) =
    withC[Task, Unit] { c =>
      Task.fork {
        Task.async[Unit] { cb =>
          Command.puts(c, settings, m, cb)
        }
      }
    }

  def put[A](settings: Settings, name: String, value: A)(implicit encoder: Encoder[(String, String, A)]) =
    withC[Task, Unit] { c =>
      Task.fork {
        Task.async[Unit] { cb =>
          Command.put(c, settings, name, value, cb)
        }
      }
    }

  def all[R](settings: Settings)(implicit decoder: Decoder[R]) =
    withC[Task, Option[R]] { c =>
      Task.fork {
        Task.async[Option[R]] { cb =>
          Command.all(c, settings, cb)
        }
      }
    }

  def delete(settings: Settings, name: String)(implicit encoder: Encoder[(String, String)]) =
    withC[Task, Unit] { c =>
      Task.async[Unit] { cb =>
        Command.delete(c, settings, name, cb)
      }
    }

  def deleteBin(settings: Settings)(implicit encoder: Encoder[String]) =
    withC[Task, Unit] { c =>
      Task.fork {
        Task.async[Unit] { cb =>
          Command.deleteBin(c, settings, cb)
        }
      }
    }
}
