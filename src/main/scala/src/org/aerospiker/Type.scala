package org.aerospiker

case class Key(
  namespace: String,
  set: String,
  key: String)

case class Bin(
  name: String,
  value: String)
