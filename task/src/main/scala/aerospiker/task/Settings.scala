package aerospiker
package task

final case class Settings(
  namespace: String,
  setName: String,
  key: String = "",
  binName: String = ""
)
