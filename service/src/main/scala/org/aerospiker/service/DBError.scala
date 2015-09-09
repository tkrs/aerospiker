package org.aerospiker.service

sealed trait DBError extends Exception
case class PutError(key: String) extends DBError
case class DeleteError(key: String) extends DBError
case class NoSuchKey(key: String) extends DBError

