package aerospiker
package listener

import aerospiker.Record
import com.aerospike.client.AerospikeException

trait RecordArrayListener[T] {
  def onSuccess(keys: Array[Key], records: Array[Record[T]]): Unit
  def onFailure(exception: AerospikeException): Unit
}
