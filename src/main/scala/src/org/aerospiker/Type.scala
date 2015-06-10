package org.aerospiker

case class Record(
  bins: Map[String, Any],
  generation: Int,
  expiration: Int)

case class Key(
  namespace: String,
  set: String,
  key: String)

case class Bin[A](
  name: String,
  value: Value[A])

case class Value[A](value: A)
