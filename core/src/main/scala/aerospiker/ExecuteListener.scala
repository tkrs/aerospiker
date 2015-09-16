package aerospiker

import com.aerospike.client.AerospikeException

trait ExecuteListener[A] {
  def onSuccess(key: Key, a: Option[Record[A]]): Unit
  def onFailure(e: AerospikeException): Unit
}
