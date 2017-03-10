package aerospiker

import cats.MonadError
import cats.data.Kleisli

trait Functions {
  private type ME[F[_]] = MonadError[F, Throwable]
  def pure[F[_]: ME, A](a: A): Action[F, A] = Kleisli.pure(a)
  def lift[F[_]: ME, A](fa: F[A]): Action[F, A] = Kleisli.lift(fa)
}

