package org.aerospiker

case class Host(
  name: String,
  port: Int)

// TODO: More support properties!
case class Settings(
  host: Array[Host] = Array(Host("127.0.0.1", 3000)),
  user: String = "",
  pwd: String = "",
  maxRetries: Int = 3)

