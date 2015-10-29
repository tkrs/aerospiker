package aerospiker

import cats.data.ReaderT
import scalaz.concurrent.Task

abstract class Functions {
  def withClient[U](f: AerospikeClient => Task[U]): Action[U] = ReaderT.function(f)
}

