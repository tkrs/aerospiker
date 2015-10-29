package aerospiker
package listener

import aerospiker.data._
import com.aerospike.client.AerospikeException

trait ExecuteListener[A] {
  def onSuccess(key: Key, a: Option[Record[A]]): Unit
  def onFailure(e: AerospikeException): Unit
}
