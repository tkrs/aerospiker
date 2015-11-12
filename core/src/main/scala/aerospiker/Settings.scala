package aerospiker

final case class Settings(
  namespace: String,
  setName: String,
  key: String = "",
  binName: String = ""
)
