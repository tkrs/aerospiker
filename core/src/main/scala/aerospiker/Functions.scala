package aerospiker

import aerospiker.listener._
import cats.MonadError
import cats.data.Kleisli
import cats.syntax.either._
import com.aerospike.client.AerospikeException
import io.circe.{ Decoder, Encoder }

import scala.collection.generic.CanBuildFrom

trait Functions {
  private type ME[F[_]] = MonadError[F, Throwable]
  def pure[F[_]: ME, A](a: A): Action[F, A] = Kleisli.pure(a)
  def lift[F[_]: ME, A](fa: F[A]): Action[F, A] = Kleisli.lift(fa)

  type Func[A] = Either[Throwable, A] => Unit

  protected def getAsync[A: Decoder](
    fa: Func[A],
    settings: Settings,
    binNames: String*
  )(implicit c: AerospikeClient): Unit

  protected def putAsync[A: Encoder](
    fa: Func[Unit],
    settings: Settings, bins: A
  )(implicit c: AerospikeClient): Unit

  protected def deleteAsync(
    fa: Func[Boolean],
    settings: Settings
  )(implicit c: AerospikeClient): Unit

  protected def allAsync[C[_], A: Decoder](
    fa: Func[C[(Key, Option[Record[A]])]],
    settings: Settings,
    binNames: String*
  )(
    implicit
    c: AerospikeClient,
    cbf: CanBuildFrom[Nothing, (Key, Option[Record[A]]), C[(Key, Option[Record[A]])]]
  ): Unit

  protected def existsAsync(
    fa: Func[Boolean],
    settings: Settings
  )(implicit c: AerospikeClient): Unit
}

private[aerospiker] trait Functions0 extends Functions {

  protected def getAsync[A: Decoder](
    fa: Func[A],
    settings: Settings,
    binNames: String*
  )(implicit c: AerospikeClient): Unit = try {
    Command.get[A](c, settings, binNames,
      Some(new RecordListener[A] {
        override def onFailure(e: AerospikeException): Unit = fa(GetError(settings.key, e).asLeft)
        override def onSuccess(key: Key, record: Option[Record[A]]): Unit = record match {
          case None => fa(NoSuchKey(settings.key).asLeft)
          case Some(r) => r.bins match {
            case None => fa(GetError(settings.key).asLeft)
            case Some(bins) => fa(bins.asRight)
          }
        }
      }))
  } catch {
    case e: Throwable => fa(e.asLeft)
  }

  protected def putAsync[A: Encoder](
    fa: Func[Unit],
    settings: Settings, bins: A
  )(implicit c: AerospikeClient): Unit = try {
    Command.put(c, settings, bins,
      Some(new WriteListener {
        override def onFailure(e: AerospikeException): Unit = fa(PutError(settings.key, e).asLeft)
        override def onSuccess(key: Key): Unit = fa(().asRight)
      }))
  } catch {
    case e: Throwable => fa(e.asLeft)
  }

  protected def deleteAsync(
    fa: Func[Boolean],
    settings: Settings
  )(implicit c: AerospikeClient): Unit = try {
    Command.delete(c, settings,
      Some(new DeleteListener {
        override def onFailure(e: AerospikeException): Unit = fa(DeleteError(settings.key, e).asLeft)
        override def onSuccess(key: Key, exists: Boolean): Unit = fa(exists.asRight)
      }))
  } catch {
    case e: Throwable => fa(e.asLeft)
  }

  protected def allAsync[C[_], A: Decoder](
    fa: Func[C[(Key, Option[Record[A]])]],
    settings: Settings,
    binNames: String*
  )(
    implicit
    c: AerospikeClient,
    cbf: CanBuildFrom[Nothing, (Key, Option[Record[A]]), C[(Key, Option[Record[A]])]]
  ): Unit = try {
    val builder = cbf.apply()
    Command.all[A](c, settings, binNames,
      Some(new RecordSequenceListener[A] {
        def onRecord(key: Key, record: Option[Record[A]]): Unit = builder += key -> record
        def onFailure(e: AerospikeException): Unit = fa(e.asLeft)
        def onSuccess(): Unit = fa(builder.result().asRight)
      }))
  } catch {
    case e: Throwable => fa(e.asLeft)
  }

  def existsAsync(
    fa: Func[Boolean],
    settings: Settings
  )(implicit c: AerospikeClient): Unit = try {
    Command.exists(c, settings,
      Some(new ExistsListener {
        def onFailure(e: AerospikeException): Unit = fa(e.asLeft)
        def onSuccess(key: Key, exists: Boolean): Unit = fa(exists.asRight)
      }))
  } catch {
    case e: Throwable => fa(e.asLeft)
  }
}

