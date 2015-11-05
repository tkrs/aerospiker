package aerospiker
package listener

import aerospiker.Record
import com.aerospike.client.AerospikeException

trait RecordSequenceListener[A] {
  def onRecord(key: Key, record: Option[Record[A]]): Unit
  def onSuccess(): Unit
  def onFailure(exception: AerospikeException): Unit
}
