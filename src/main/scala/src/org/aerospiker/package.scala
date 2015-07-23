package org

package object aerospiker {

  type Host = com.aerospike.client.Host

  final case class Record(
    bins: Map[String, Any],
    generation: Int,
    expiration: Int)

  final case class Key(
    namespace: String,
    set: String,
    key: String)

  final case class Bin[A](
    name: String,
    value: Value[A])

  final case class Value[A](
    value: A)
}
