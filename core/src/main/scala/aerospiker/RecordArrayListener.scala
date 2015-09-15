package aerospiker

import com.aerospike.client.AerospikeException

/**
 * Asynchronous result notifications for batch get commands.
 * The result is sent in a single array.
 */
trait RecordArrayListener[T] {
  /**
   * This method is called when an asynchronous batch get command completes successfully.
   * The returned record array is in positional order with the original key array order.
   *
   * @param keys			unique record identifiers
   * @param records		record instances, an instance will be null if the key is not found
   */
  def onSuccess(keys: Array[Key], records: Array[Record[T]]): Unit

  /**
   * This method is called when an asynchronous batch get command fails.
   *
   * @param exception		error that occurred
   */
  def onFailure(exception: AerospikeException): Unit
}
