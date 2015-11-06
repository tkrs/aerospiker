package aerospiker

import cats.data.ReaderT

trait Functions {
  type C
  def withC[F[_], U](f: C => F[U]): Action[F, C, U] = ReaderT.function[F, C, U](f)
}

