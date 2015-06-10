package org.aerospiker

// TODO: More support properties!
case class Settings(
  host: String = "127.0.0.1:3000",
  user: String = "",
  pwd: String = "",
  maxRetries: Int = 3)

