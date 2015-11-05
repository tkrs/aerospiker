package aerospiker

import cats.data.ReaderT
import scalaz.concurrent.Task

abstract class Functions {
  def withClient[F[_], U](f: AerospikeClient => F[U]): Action[F, U] = ReaderT.function(f)
}

