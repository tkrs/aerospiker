package aerospiker

import cats.data.ReaderT

trait Functions {
  def withClient[F[_], U](f: AerospikeClient => F[U]): Action[F, U] = ReaderT.function[F, AerospikeClient, U](f)
}

