package aerospiker
package listener

import com.aerospike.client.AerospikeException

trait WriteListener {
  def onSuccess(key: Key): Unit
  def onFailure(e: AerospikeException): Unit
}
