package aerospiker
package task
package monix

import _root_.monix.eval.Task
import _root_.monix.execution.Cancelable
import io.circe.{ Decoder, Encoder }

import scala.collection.generic.CanBuildFrom

trait Aerospike {
  def get[A: Decoder](settings: Settings, binNames: String*): Action[Task, A]
  def put[A: Encoder](settings: Settings, bins: A): Action[Task, Unit]
  def delete(settings: Settings): Action[Task, Boolean]
  def all[C[_], A](settings: Settings, binNames: String*)(
    implicit
    decoder: Decoder[A],
    cbf: CanBuildFrom[Nothing, (Key, Option[Record[A]]), C[(Key, Option[Record[A]])]]
  ): Action[Task, C[(Key, Option[Record[A]])]]
  def exists(settings: Settings): Action[Task, Boolean]
}

object Aerospike {

  def apply(): Aerospike = new Impl with Functions0

  class Impl extends Aerospike { self: Functions =>

    def get[A: Decoder](settings: Settings, binNames: String*): Action[Task, A] =
      Action[Task, A] { implicit c =>
        Task.async[A] { (s, cb) =>
          s.execute(new Runnable {
            override def run(): Unit =
              getAsync[A](_.fold(cb.onError, cb.onSuccess), settings, binNames: _*)
          })
          Cancelable.empty
        }
      }

    def put[A: Encoder](settings: Settings, bins: A): Action[Task, Unit] =
      Action[Task, Unit] { implicit c =>
        Task.async[Unit] { (s, cb) =>
          s.execute(new Runnable {
            override def run(): Unit =
              putAsync[A](_.fold(cb.onError, cb.onSuccess), settings, bins)
          })
          Cancelable.empty
        }
      }

    //  def puts[A](settings: Settings, kv: Map[String, A])(implicit encoder: Encoder[A]): Action[Task, Seq[String]] =
    //    kv.toList.traverse { case (k: String, v: A) => put(settings.copy(key = k), v).map(_ => k) }

    def delete(settings: Settings): Action[Task, Boolean] =
      Action[Task, Boolean] { implicit c =>
        Task.async[Boolean] { (s, cb) =>
          s.execute(new Runnable {
            override def run(): Unit =
              deleteAsync(_.fold(cb.onError, cb.onSuccess), settings)
          })
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
      Action[Task, C[(Key, Option[Record[A]])]] { implicit c =>
        Task.async[C[(Key, Option[Record[A]])]] { (s, cb) =>
          s.execute(new Runnable {
            override def run(): Unit =
              allAsync[C, A](_.fold(cb.onError, cb.onSuccess), settings, binNames: _*)
          })
          Cancelable.empty
        }
      }

    def exists(settings: Settings): Action[Task, Boolean] =
      Action[Task, Boolean] { implicit c =>
        Task.async[Boolean] { (s, cb) =>
          s.execute(new Runnable {
            override def run(): Unit =
              existsAsync(_.fold(cb.onError, cb.onSuccess), settings)
          })
          Cancelable.empty
        }
      }
  }
}
