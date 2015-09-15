package aerospiker

import com.aerospike.client.AerospikeException

/**
 * Asynchronous result notifications for batch get and scan commands.
 * The results are sent one record at a time.
 */
trait RecordSequenceListener[A] {
  /**
   * This method is called when an asynchronous record is received from the server.
   * The receive sequence is not ordered.
   * <p>
   * The user may throw a
   * {@link com.aerospike.client.AerospikeException.QueryTerminated AerospikeException.QueryTerminated}
   * exception if the command should be aborted.  If any exception is thrown, parallel command threads
   * to other nodes will also be terminated and the exception will be propagated back through the
   * commandFailed() call.
   *
   * @param key					unique record identifier
   * @param record				record instance, will be null if the key is not found
   * @throws AerospikeException	if error occurs or scan should be terminated.
   */
  @throws(classOf[AerospikeException])
  def onRecord(key: Key, record: Option[Record[A]]): Unit

  /**
   * This method is called when the asynchronous batch get or scan command completes.
   */
  def onSuccess(): Unit

  /**
   * This method is called when an asynchronous batch get or scan command fails.
   *
   * @param exception				error that occurred
   */
  def onFailure(exception: AerospikeException): Unit
}
