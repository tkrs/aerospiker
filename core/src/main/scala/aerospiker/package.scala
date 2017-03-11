import aerospiker.protocol.JsonPacker
import cats.MonadError
import cats.data.Kleisli

package object aerospiker {

  private[aerospiker] val packer = JsonPacker()

  type Action[F[_], U] = Kleisli[F, AerospikeClient, U]
  def Action[F[_], U](f: AerospikeClient => F[U])(
    implicit
    F: MonadError[F, Throwable]
  ): Action[F, U] = Kleisli[F, AerospikeClient, U](f)

  type Bin = com.aerospike.client.Bin
  object Bin {

  }

  type Value = com.aerospike.client.Value
  object Value {

  }

  type Host = com.aerospike.client.Host
  object Host {
    def apply(name: String, port: Int) = new Host(name, port)
  }

  type Key = com.aerospike.client.Key
  object Key {
    def apply(namespace: String, set: String, key: String) = new Key(namespace, set, key)
  }

}

package aerospiker {

  final case class Record[T](bins: Option[T], generation: Int, expiration: Int)

  final case class Settings(
    namespace: String,
    setName: String
  )
}
