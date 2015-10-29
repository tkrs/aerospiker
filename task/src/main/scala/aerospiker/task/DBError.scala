package aerospiker.task

sealed abstract class DBError(cause: Throwable = null) extends Exception(cause)
case class PutError(key: String, cause: Throwable = null) extends DBError(cause)
case class DeleteError(key: String, cause: Throwable = null) extends DBError(cause)
case class NoSuchKey(key: String, cause: Throwable = null) extends DBError(cause)
case class GetError(key: String, cause: Throwable = null) extends DBError(cause)
