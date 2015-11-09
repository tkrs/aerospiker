package aerospiker
package task

import com.aerospike.client.AerospikeException

import aerospiker.{ LargeMapCommand => Command }
import aerospiker.listener._
import io.circe.{ Encoder, Decoder }

import scalaz.{ \/-, -\/ }
import scalaz.concurrent.Task

object AerospikeLargeMap extends Functions {

  type C = AerospikeClient

  def get[R](settings: Settings, a: String)(implicit decoder: Decoder[R]) =
    withC[Task, Option[R]] { c =>
      implicit val dec = Decoder[Map[String, Map[String, R]]]
      implicit val enc = Encoder[Seq[String]]
      Task.fork {
        Task.async[Option[R]] { cb =>
          try {
            Command.get(c, settings, a,
              Some(new ExecuteListener[Map[String, Map[String, R]]] {
                override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
                override def onSuccess(key: Key, rec: Option[Record[Map[String, Map[String, R]]]]): Unit = {
                  val o = rec.collect { case Record(Some(x), _, _) => x }
                  val x = o getOrElse Map.empty
                  val v = x.get("SUCCESS") flatMap (_ get a)
                  cb(\/-(v))
                }
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }

  def puts[A](settings: Settings, m: A)(implicit encoder: Encoder[(String, A)]) =
    withC[Task, Unit] { c =>
      Task.fork {
        Task.async[Unit] { cb =>
          try {
            Command.puts(c, settings, m,
              Some(new ExecuteListener[Map[String, Int]] {
                override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
                override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
                  val o = a.collect { case Record(Some(x), _, _) => x }
                  val x = o getOrElse Map.empty
                  if (x.contains("SUCCESS")) cb(\/-({}))
                  else if (x.contains("FAILURE")) cb(-\/(new AerospikeException(a.toString)))
                  else cb(-\/(new AerospikeException("Invalid UDF return value")))
                }
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }

  def put[A](settings: Settings, name: String, value: A)(implicit encoder: Encoder[(String, String, A)]) =
    withC[Task, Unit] { c =>
      Task.fork {
        Task.async[Unit] { cb =>
          try {
            Command.put(c, settings, name, value,
              Some(new ExecuteListener[Map[String, Int]] {
                override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
                override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
                  val o = a.collect { case Record(Some(x), _, _) => x }
                  val x = o getOrElse Map.empty
                  if (x.contains("SUCCESS"))
                    cb(\/-({}))
                  else if (x.contains("FAILURE"))
                    cb(-\/(new AerospikeException(a.toString)))
                  else
                    cb(-\/(new AerospikeException("Invalid UDF return value")))
                }
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }

  def all[R](settings: Settings)(implicit decoder: Decoder[R]) =
    withC[Task, Option[R]] { c =>
      Task.fork {
        Task.async[Option[R]] { cb =>
          try {
            Command.all(c, settings,
              Some(new ExecuteListener[Map[String, R]] {
                override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
                override def onSuccess(key: Key, a: Option[Record[Map[String, R]]]): Unit = {
                  val o = a.collect { case Record(Some(x), _, _) => x }
                  val x = o getOrElse Map.empty
                  cb(\/-(x.get("SUCCESS")))
                }
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }

  def delete(settings: Settings, name: String)(implicit encoder: Encoder[(String, String)]) =
    withC[Task, Unit] { c =>
      Task.async[Unit] { cb =>
        try {
          Command.delete(c, settings, name,
            Some(new ExecuteListener[Map[String, Int]] {
              override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
              override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
                val o = a.collect { case Record(Some(x), _, _) => x }
                val x = o getOrElse Map.empty
                if (x.contains("SUCCESS"))
                  cb(\/-({}))
                else if (x.contains("FAILURE"))
                  cb(-\/(new AerospikeException(a.toString)))
                else
                  cb(-\/(new AerospikeException("Invalid UDF return value")))
              }
            }))
        } catch {
          case e: Throwable => cb(-\/(e))
        }
      }
    }

  def deleteBin(settings: Settings)(implicit encoder: Encoder[String]) =
    withC[Task, Unit] { c =>
      Task.fork {
        Task.async[Unit] { cb =>
          try {
            Command.deleteBin(c, settings,
              Some(new ExecuteListener[Map[String, Int]] {
                override def onFailure(e: AerospikeException): Unit = cb(-\/(e))
                override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
                  val o = a.collect { case Record(Some(x), _, _) => x }
                  val x = o getOrElse Map.empty
                  if (x.contains("SUCCESS"))
                    cb(\/-({}))
                  else if (x.contains("FAILURE"))
                    cb(-\/(new AerospikeException(a.toString)))
                  else
                    cb(-\/(new AerospikeException("Invalid UDF return value")))
                }
              }))
          } catch {
            case e: Throwable => cb(-\/(e))
          }
        }
      }
    }
}
