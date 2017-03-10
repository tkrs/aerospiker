package aerospiker

final case class Record[T](bins: Option[T], generation: Int, expiration: Int)
