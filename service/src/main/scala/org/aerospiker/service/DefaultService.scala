package org.aerospiker.service

import cats._
import cats.data.{ NonEmptyList => NEL, Xor }
import cats.std.all._
import com.typesafe.scalalogging.LazyLogging
import org.aerospiker.{ Client, Key, Record }
import org.aerospiker.policy.ClientPolicy

abstract class DefaultService(val client: Client)(implicit dp: ClientPolicy) extends DefaultType with LazyLogging {

  import scalaz.concurrent.Task
  import scalaz.{ -\/, \/, \/- }

  implicit val nelSemigroup: Semigroup[NEL[Throwable]] = SemigroupK[NEL].algebra[Throwable]

  protected def namespace: String
  protected def setName: String

  override def get[M](key: String)(implicit decoder: D[M]): Throwable Xor Option[M] = {
    val k = Key(namespace, setName, key)
    client.get(k).run.run match {
      case -\/(e) => Xor.left(e)
      case \/-(r) => r match {
        case Some(record) => Xor.right(decoder(record.bins))
        case None => Xor.left(NoSuchKey(key))
      }
    }
  }

  override def put[M](key: String, m: M)(implicit encoder: E[M]): Throwable Xor Unit = {
    val k = Key(namespace, setName, key)
    val bins = encoder(m)
    val ret = client.put(k, bins: _*).run.run
    ret match {
      case -\/(e) => Xor.left(e)
      case \/-(_) => Xor.right({})
    }
  }

  def toXor[A, B](x: A \/ B): A Xor B = x.fold(Xor.left(_), Xor.right(_))

  override def puts[M](kvs: Map[String, M])(implicit encoder: E[M]): Task[Seq[Throwable Xor String]] = {
    Task.gatherUnordered {
      kvs map {
        case (k, v) =>
          val key = Key(namespace, setName, k)
          client.put(key, encoder(v): _*).bimap({ e => PutError(k) }, { Unit => k }).run map (toXor(_))
      } toSeq
    }
  }

  override def all[M](target: String, binNames: Seq[String])(implicit decode: D[M]): Throwable Xor Seq[(Key, Option[M])] = {
    val all = client.scanAll(namespace, setName, binNames: _*).map(toXor(_)).run
    all map { kvs: Seq[(Key, Option[Record])] =>
      kvs map {
        case (k: Key, record: Option[Record]) => {
          record match {
            case Some(v) => (k, decode(v.bins))
            case None => (k, None)
          }
        }
      }
    }
  }

  override def delete[T](keyName: String): Xor[Throwable, Unit] = {
    val key = Key(namespace, setName, keyName)
    client.delete(key).run.run match {
      case -\/(e) => Xor.left(e)
      case \/-(_) => Xor.right(Unit)
    }
  }

  override def deletes[T](keys: Seq[String]): Task[Seq[Throwable Xor Boolean]] = {
    Task.gatherUnordered {
      keys map { k =>
        val key = Key(namespace, setName, k)
        client.delete(key).leftMap(e => DeleteError(k)).run map (toXor(_))
      }
    }
  }
}

