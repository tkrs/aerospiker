package aerospiker

import com.aerospike.client.AerospikeException

trait RecordListener[A] {
  def onSuccess(key: Key, record: Option[Record[A]]): Unit
  def onFailure(exception: AerospikeException): Unit
}
