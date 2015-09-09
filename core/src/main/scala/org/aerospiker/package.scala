package org.aerospiker

import policy.ClientPolicy

object `package` {

  type Host = com.aerospike.client.Host
  type Key = com.aerospike.client.Key
  type Bin = com.aerospike.client.Bin
  type Value = com.aerospike.client.Value

  type BinName = String
  type LDataKey = String
  type LDValue = Map[String, Any]

  object ClientFactory {
    def apply(host: String, port: Int)(implicit policy: ClientPolicy): Client = {
      Client(Seq(Host(host, port)))
    }
  }

}

object Host {
  def apply(name: String, port: Int) = new Host(name, port)
}

object Key {
  def apply(namespace: String, set: String, key: String) = new Key(namespace, set, key)
}

object Bin {
  def apply(name: String, value: Value) = new Bin(name, value)
}

final case class Empty()

final case class Record(
  bins: Map[String, Any],
  generation: Int,
  expiration: Int
)
