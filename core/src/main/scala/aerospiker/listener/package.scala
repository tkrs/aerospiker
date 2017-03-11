package aerospiker

import com.aerospike.client.AerospikeException

package listener {

  trait DeleteListener {
    def onSuccess(key: Key, existed: Boolean): Unit
    def onFailure(e: AerospikeException): Unit
  }

  trait ExecuteListener[A] {
    def onSuccess(key: Key, a: Option[Record[A]]): Unit
    def onFailure(e: AerospikeException): Unit
  }

  trait ExistsListener {
    def onSuccess(key: Key, exists: Boolean): Unit
    def onFailure(e: AerospikeException): Unit
  }

  trait RecordArrayListener[T] {
    def onSuccess(keys: Array[Key], records: Array[Record[T]]): Unit
    def onFailure(exception: AerospikeException): Unit
  }

  trait RecordListener[A] {
    def onSuccess(key: Key, record: Option[Record[A]]): Unit
    def onFailure(exception: AerospikeException): Unit
  }

  trait RecordSequenceListener[A] {
    def onRecord(key: Key, record: Option[Record[A]]): Unit
    def onSuccess(): Unit
    def onFailure(exception: AerospikeException): Unit
  }

  trait WriteListener {
    def onSuccess(key: Key): Unit
    def onFailure(e: AerospikeException): Unit
  }
}
