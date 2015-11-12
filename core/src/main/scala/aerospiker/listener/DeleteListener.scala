package aerospiker
package listener

import com.aerospike.client.AerospikeException

trait DeleteListener {
  def onSuccess(key: Key, existed: Boolean): Unit
  def onFailure(e: AerospikeException): Unit
}
