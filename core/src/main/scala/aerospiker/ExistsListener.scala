package aerospiker

import com.aerospike.client.AerospikeException

trait ExistsListener {
  def onSuccess(key: Key, exists: Boolean): Unit
  def onFailure(e: AerospikeException): Unit
}
