package org

package object aerospiker {

  object Host {
    def apply(name: String, port: Int) = new Host(name, port)
  }
  type Host = com.aerospike.client.Host

  object Key {
    def apply(namespace: String, set: String, key: String) = new Key(namespace, set, key)
  }
  type Key = com.aerospike.client.Key

  object Bin {
    def apply(name: String, value: Value) = new Bin(name, value)
  }
  type Bin = com.aerospike.client.Bin

  type Value = com.aerospike.client.Value

  case class Empty()

  final case class Record(
    bins: Map[String, Any],
    generation: Int,
    expiration: Int)

}
