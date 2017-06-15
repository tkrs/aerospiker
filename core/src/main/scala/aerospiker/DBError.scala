package aerospiker

sealed abstract class DBError(cause: Throwable) extends Exception(cause) {
  override def fillInStackTrace(): Throwable = cause.fillInStackTrace()
}
case class PutError(key: String, cause: Throwable = null) extends DBError(cause)
case class DeleteError(key: String, cause: Throwable = null) extends DBError(cause)
case class NoSuchKey(key: String, cause: Throwable = null) extends DBError(cause)
case class GetError(key: String, cause: Throwable = null) extends DBError(cause)
