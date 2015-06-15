package org.aerospiker

case class Host(
  name: String,
  port: Int)

// TODO: Support more properties!
case class Settings(
  host: Seq[Host] = List(Host("127.0.0.1", 3000)),
  user: String = "",
  pwd: String = "",
  maxRetries: Int = 3)

