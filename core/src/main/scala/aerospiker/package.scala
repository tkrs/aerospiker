import aerospiker.buffer.Buffer
import cats.data.ReaderT

package object aerospiker {

  type Action[F[_], C, U] = ReaderT[F, C, U]

  type Host = com.aerospike.client.Host
  type Key = com.aerospike.client.Key
  type Bin = com.aerospike.client.Bin
  type Value = com.aerospike.client.Value

  object Host {
    def apply(name: String, port: Int) = new Host(name, port)
  }

  object Key {
    def apply(namespace: String, set: String, key: String) = new Key(namespace, set, key)
  }

}
