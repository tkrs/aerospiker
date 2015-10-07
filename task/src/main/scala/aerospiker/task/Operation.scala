package aerospiker.task

import aerospiker._
import cats.data.{ NonEmptyList => NEL, Xor }, Xor._, cats.std.all._
import cats.{ Semigroup, SemigroupK }
import com.aerospike.client.AerospikeException
import com.typesafe.scalalogging.LazyLogging
import io.circe.{ Decoder, Encoder }
import shapeless._

sealed trait Operation

sealed trait DBError extends Exception
case class PutError(key: String) extends DBError
case class DeleteError(key: String) extends DBError
case class NoSuchKey(key: String) extends DBError

abstract class Aerospike(command: AsyncCommandExecutor) extends Operation with LazyLogging {

  import scalaz.concurrent.Task
  import scalaz.{ -\/, \/- }

  implicit val nelSemigroup: Semigroup[NEL[Throwable]] = SemigroupK[NEL].algebra[Throwable]

  protected def namespace: String
  protected def setName: String

  def get[A](key: String, binNames: String*)(
    implicit
    decoder: Decoder[A]
  ): Task[Throwable Xor Option[A]] = {
    val t = Task.async[Option[Record[A]]] { register =>
      command.get[A](
        Some(new RecordListener[A] {
          def onSuccess(key: Key, record: Option[Record[A]]): Unit = {
            register(\/-(record))
          }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        }),
        Key(namespace, setName, key),
        binNames: _*
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(e))
      case \/-(v) => v match {
        case None => Task(Xor.left(NoSuchKey(key)))
        case Some(record) => Task(Xor.right(record.bins))
      }
    }
  }

  def put[A](key: String, bins: A)(
    implicit
    encoder: Encoder[A]
  ): Task[Throwable Xor String] = {
    val t = Task.async[Unit] { register =>
      command.put[A](
        Some(new WriteListener {
          def onSuccess(key: Key): Unit = { register(\/-({})) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        }),
        Key(namespace, setName, key),
        bins
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(PutError(key)))
      case \/-(v) => Task(Xor.right(key))
    }
  }

  def puts[A](kvs: Map[String, A])(implicit encoder: Encoder[A]): Task[Seq[Throwable Xor String]] = {
    Task.gatherUnordered {
      kvs map {
        case (k, v) => put(k, v)
      } toSeq
    }
  }

  def all[A](binNames: String*)(
    implicit
    decoder: Decoder[A]
  ): Task[Throwable Xor Seq[(Key, Option[Record[A]])]] = {
    val t = Task.async[Seq[(Key, Option[Record[A]])]] { register =>
      import scala.collection.mutable.ListBuffer
      val buffer: ListBuffer[(Key, Option[Record[A]])] = ListBuffer.empty
      command.scanAll[A](
        Some(new RecordSequenceListener[A] {
          def onRecord(key: Key, record: Option[Record[A]]): Unit = buffer += key -> record
          def onFailure(e: AerospikeException): Unit = register(-\/(e))
          def onSuccess(): Unit = register(\/-(buffer.toSeq))
        }),
        namespace,
        setName,
        binNames: _*
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(e))
      case \/-(v) => Task(Xor.right(v))
    }
  }

  def delete(key: String): Task[DeleteError Xor Boolean] = {
    val t = Task.async[Boolean] { register =>
      command.delete(
        Some(new DeleteListener {
          def onSuccess(key: Key, existed: Boolean): Unit = { register(\/-(existed)) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        }),
        Key(namespace, setName, key)
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(DeleteError(key)))
      case \/-(v) => Task(Xor.right(v))
    }
  }

  def deletes(keys: Seq[String]): Task[Seq[DeleteError Xor Boolean]] = {
    Task.gatherUnordered {
      keys map { k =>
        delete(k) map {
          case Left(e) => Xor.left(e)
          case l @ Right(v) => l
        }
      }
    }
  }

  def exists(key: Key): Task[Throwable Xor Boolean] = {
    val t = Task.async[Boolean] { register =>
      command.exists(
        Some(new ExistsListener {
          def onSuccess(key: Key, exists: Boolean): Unit = { register(\/-(exists)) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        }),
        key
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(e))
      case \/-(v) => Task(Xor.right(v))
    }
  }
}

abstract class AerospikeLargeMap(command: AsyncCommandExecutor, createModule: Option[String] = None) extends Operation with LazyLogging {

  import scalaz.concurrent.Task
  import scalaz.{ -\/, \/- }

  protected def namespace: String
  protected def setName: String
  protected def key: String
  protected def binName: String

  private[this] val PackageName = "lmap"

  def get[R](a: String)(
    implicit
    decoder: Decoder[Map[String, Map[String, R]]]
  ): Task[NoSuchKey Xor Option[R]] = {
    val t = Task.async[Option[R]] { register =>
      command.execute[Seq[String], Map[String, Map[String, R]]](
        Some(new ExecuteListener[Map[String, Map[String, R]]] {
          override def onFailure(e: AerospikeException): Unit = {
            register(-\/(e))
          }
          override def onSuccess(key: Key, rec: Option[Record[Map[String, Map[String, R]]]]): Unit = {
            val o = rec.collect {
              case Record(Some(x), _, _) => x
            }
            val x = o getOrElse Map.empty
            val v = x.get("SUCCESS") flatMap (_ get a)
            register(\/-(v))
          }
        }),
        Key(namespace, setName, key),
        PackageName,
        "get",
        binName :: a :: Nil
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(NoSuchKey(a)))
      case \/-(v) => Task(Xor.right(v))
    }
  }

  def puts[A](m: A)(
    implicit
    encoder: Encoder[String :: A :: HNil]
  ): Task[PutError Xor Unit] = {
    val t = Task.async[Unit] { register =>
      try {
        command.execute[String :: A :: HNil, Map[String, Int]](
          Some(new ExecuteListener[Map[String, Int]] {
            override def onFailure(e: AerospikeException): Unit = register(-\/(e))
            override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
              val o = a.collect {
                case Record(Some(x), _, _) => x
              }
              val x = o getOrElse (Map.empty)
              if (x.contains("SUCCESS"))
                register(\/-({}))
              else if (x.contains("FAILURE"))
                register(-\/(new AerospikeException(a.toString)))
              else
                register(-\/(new AerospikeException("Invalid UDF return value")))
            }
          }),
          Key(namespace, setName, key),
          PackageName,
          "put_all",
          binName :: m :: HNil
        )
      } catch {
        case e: Throwable =>
          logger.error("puts", e)
          throw e
      }
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(PutError(m.toString)))
      case \/-(v) => Task(Xor.right(v))
    }
  }

  def put[A](name: String, value: A)(
    implicit
    encoder: Encoder[String :: String :: A :: HNil]
  ): Task[PutError Xor Unit] = {
    val t = Task.async[Unit] { register =>
      command.execute[String :: String :: A :: HNil, Map[String, Int]](
        Some(new ExecuteListener[Map[String, Int]] {
          override def onFailure(e: AerospikeException): Unit = register(-\/(e))
          override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
            val o = a.collect {
              case Record(Some(x), _, _) => x
            }
            val x = o getOrElse (Map.empty)
            if (x.contains("SUCCESS"))
              register(\/-({}))
            else if (x.contains("FAILURE"))
              register(-\/(new AerospikeException(a.toString)))
            else
              register(-\/(new AerospikeException("Invalid UDF return value")))
          }
        }),
        Key(namespace, setName, key),
        PackageName,
        "put",
        binName :: name :: value :: HNil
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(PutError(value.toString)))
      case \/-(v) => Task(Xor.right(v))
    }
  }

  def all[R]()(
    implicit
    decoder: Decoder[R]
  ): Task[Throwable Xor Option[R]] = {
    val t = Task.async[Option[R]] { register =>
      command.execute[Seq[String], Map[String, R]](
        Some(new ExecuteListener[Map[String, R]] {
          override def onFailure(e: AerospikeException): Unit = register(-\/(e))
          override def onSuccess(key: Key, a: Option[Record[Map[String, R]]]): Unit = {
            val o = a.collect {
              case Record(Some(x), _, _) => x
            }
            val x = o getOrElse Map.empty
            register(\/-(x.get("SUCCESS")))
          }
        }),
        Key(namespace, setName, key),
        PackageName,
        "scan",
        binName :: Nil
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(e))
      case \/-(v) => Task(Xor.right(v))
    }
  }

  def delete(name: String)(
    implicit
    encoder: Encoder[String :: String :: HNil]
  ): Task[DeleteError Xor Unit] = {
    val t = Task.async[Unit] { register =>
      command.execute[String :: String :: HNil, Map[String, Int]](
        Some(new ExecuteListener[Map[String, Int]] {
          override def onFailure(e: AerospikeException): Unit = register(-\/(e))
          override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
            val o = a.collect {
              case Record(Some(x), _, _) => x
            }
            val x = o getOrElse (Map.empty)
            if (x.contains("SUCCESS"))
              register(\/-({}))
            else if (x.contains("FAILURE"))
              register(-\/(new AerospikeException(a.toString)))
            else
              register(-\/(new AerospikeException("Invalid UDF return value")))
          }
        }),
        Key(namespace, setName, key),
        PackageName,
        "remove",
        binName :: name :: HNil
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(DeleteError(name.toString)))
      case \/-(v) => Task(Xor.right(v))
    }
  }

  def deleteBin()(
    implicit
    encoder: Encoder[String :: HNil]
  ): Task[DeleteError Xor Unit] = {
    val t = Task.async[Unit] { register =>
      command.execute[String :: HNil, Map[String, Int]](
        Some(new ExecuteListener[Map[String, Int]] {
          override def onFailure(e: AerospikeException): Unit = register(-\/(e))
          override def onSuccess(key: Key, a: Option[Record[Map[String, Int]]]): Unit = {
            val o = a.collect {
              case Record(Some(x), _, _) => x
            }
            val x = o getOrElse (Map.empty)
            if (x.contains("SUCCESS"))
              register(\/-({}))
            else if (x.contains("FAILURE"))
              register(-\/(new AerospikeException(a.toString)))
            else
              register(-\/(new AerospikeException("Invalid UDF return value")))
          }
        }),
        Key(namespace, setName, key),
        PackageName,
        "destroy",
        binName :: HNil
      )
    }
    t.attempt.flatMap {
      case -\/(e) => Task(Xor.left(DeleteError(binName.toString)))
      case \/-(v) => Task(Xor.right(v))
    }
  }
}

//  def destory(): Task[Unit] = Task.delay(lmap.destroy())
//
//  def exists(keyValue: Value): Task[Boolean] = Task.delay(lmap.exists(keyValue))
//
//  def filter[A](filterModule: String, filterName: String, filterArgs: Value*)(implicit decoder: Decoder[A]): Task[Option[A]] =
//    Task.delay {
//      val ret: Map[Any, Any] = lmap.filter(filterModule, filterName, filterArgs: _*).asInstanceOf[Map[Any, Any]]
//      decoder(ret)
//    }
//
//  def get[A](name: Value)(implicit decoder: Decoder[A]): Task[Option[A]] = ???
//  // Task.delay(decoder(lmap.get(name)))
//
//  def getCapacity: Task[Int] = Task.delay(lmap.getCapacity)
//
//  def getConfig: Task[Map[_, _]] = ??? //Task.delay(lmap.getConfig)
//
//  def put[A](m: A)(implicit encoder: Encoder[A]): Task[Unit] =
//    Task.delay(lmap.put(encoder(m)))
//
//  def put(name: Value, value: Value): Task[Unit] = Task.delay(???)
//
//  def remove(name: Value): Task[Unit] = Task.delay(???)
//
//  def scan[A](implicit decoder: Decoder[A]): Task[Option[A]] = ??? //Task.delay(decoder(lmap.scan())
//
//  def setCapacity(capacity: Int): Task[Unit] = Task.delay(lmap.setCapacity(capacity))
//
//  def size(): Task[Int] = Task.delay(lmap.size())

class AerospikeLargeList extends LazyLogging
