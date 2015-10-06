package aerospiker

import com.aerospike.client.{ AerospikeException, Operation }
import com.aerospike.client.async.AsyncCluster

import io.circe.{ Decoder, Encoder }

import aerospiker.policy._

object AsyncClient {
  implicit val clientPolicy = ClientPolicy()
  def apply(hosts: Host*)(implicit policy: ClientPolicy) = new AsyncClient(policy, hosts: _*)
}

object Host {
  def apply(name: String, port: Int) = new Host(name, port)
}

object Key {
  def apply(namespace: String, set: String, key: String) = new Key(namespace, set, key)
}

/**
 * Asynchronous Aerospike client.
 * <p>
 * Your application uses this class to perform asynchronous database operations
 * such as writing and reading records, and selecting sets of records. Write
 * operations include specialized functionality such as append/prepend and arithmetic
 * addition.
 * <p>
 * This client is thread-safe. One client instance should be used per cluster.
 * Multiple threads should share this cluster instance.
 * <p>
 * Each record may have multiple bins, unless the Aerospike server nodes are
 * configured as "single-bin". In "multi-bin" mode, partial records may be
 * written or read by specifying the relevant subset of bins.
 */
class AsyncClient(policy: ClientPolicy, hosts: Host*) {
  implicit val cluster = new AsyncCluster(policy, hosts.toArray)
  cluster.initTendThread(policy.failIfNotConnected)
  /**
   * Default read policy that is used when asynchronous read command policy is null.
   */
  implicit val asyncReadPolicyDefault: Policy = policy.asyncReadPolicyDefault
  /**
   * Default write policy that is used when asynchronous write command policy is null.
   */
  implicit val asyncWritePolicyDefault: WritePolicy = policy.asyncWritePolicyDefault
  /**
   * Default scan policy that is used when asynchronous scan command policy is null.
   */
  implicit val asyncScanPolicyDefault: ScanPolicy = policy.asyncScanPolicyDefault
  /**
   * Default query policy that is used when asynchronous query command policy is null.
   */
  implicit val asyncQueryPolicyDefault: QueryPolicy = policy.asyncQueryPolicyDefault
  /**
   * Default batch policy that is used when asynchronous batch command policy is null.
   */
  implicit val asyncBatchPolicyDefault: BatchPolicy = policy.asyncBatchPolicyDefault

  def close() = cluster.close()

}

object AsyncCommandExecutor {
  def apply(client: AsyncClient) = new AsyncCommandExecutor(client)
}

class AsyncCommandExecutor(val client: AsyncClient) {
  import client._

  /**
   * Asynchronously write record bin(s).
   * This method schedules the put command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   * <p>
   * The policy specifies the transaction timeout, record expiration and how the transaction is
   * handled when the record already exists.
   *
   * @param listener				where to send results, pass in null for fire and forget
   * @param key					unique record identifier
   * @param bins					array of bin name/value pairs
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def put[T](listener: Option[WriteListener], key: Key, bins: T)(
    implicit
    policy: WritePolicy = asyncWritePolicyDefault,
    encoder: Encoder[T]
  ) =
    new AsyncWrite[T](cluster, policy, listener, key, bins, Operation.Type.WRITE).execute()

  /**
   * Asynchronously append bin string values to existing record bin values.
   * This method schedules the append command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   * <p>
   * The policy specifies the transaction timeout, record expiration and how the transaction is
   * handled when the record already exists.
   * This call only works for string values.
   *
   * @param listener				where to send results, pass in null for fire and forget
   * @param key					unique record identifier
   * @param bins					array of bin name/value pairs
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def append[T](listener: Option[WriteListener], key: Key, bins: T)(
    implicit
    policy: WritePolicy = asyncWritePolicyDefault,
    encoder: Encoder[T]
  ) =
    new AsyncWrite[T](cluster, policy, listener, key, bins, Operation.Type.APPEND).execute()

  /**
   * Asynchronously prepend bin string values to existing record bin values.
   * This method schedules the prepend command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   * <p>
   * The policy specifies the transaction timeout, record expiration and how the transaction is
   * handled when the record already exists.
   * This call works only for string values.
   *
   * @param listener				where to send results, pass in null for fire and forget
   * @param key					unique record identifier
   * @param bins					array of bin name/value pairs
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def prepend[T](listener: Option[WriteListener], key: Key, bins: T)(
    implicit
    policy: WritePolicy = asyncWritePolicyDefault,
    encoder: Encoder[T]
  ) =
    new AsyncWrite[T](cluster, policy, listener, key, bins, Operation.Type.PREPEND).execute()

  /**
   * Asynchronously add integer bin values to existing record bin values.
   * This method schedules the add command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   * <p>
   * The policy specifies the transaction timeout, record expiration and how the transaction is
   * handled when the record already exists.
   * This call only works for integer values.
   *
   * @param listener				where to send results, pass in null for fire and forget
   * @param key					unique record identifier
   * @param bins					array of bin name/value pairs
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def add[T](listener: Option[WriteListener], key: Key, bins: T)(
    implicit
    policy: WritePolicy = asyncWritePolicyDefault,
    encoder: Encoder[T]
  ) =
    new AsyncWrite[T](cluster, policy, listener, key, bins, Operation.Type.ADD).execute()

  /**
   * Asynchronously delete record for specified key.
   * This method schedules the delete command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   * <p>
   * The policy specifies the transaction timeout.
   *
   * @param listener				where to send results, pass in null for fire and forget
   * @param key					unique record identifier
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def delete(listener: Option[DeleteListener], key: Key)(
    implicit
    policy: WritePolicy = asyncWritePolicyDefault
  ) =
    new AsyncDelete(cluster, policy, listener, key).execute()

  /**
   * Asynchronously create record if it does not already exist.  If the record exists, the record's
   * time to expiration will be reset to the policy's expiration.
   * <p>
   * This method schedules the touch command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   *
   * @param listener				where to send results, pass in null for fire and forget
   * @param key					unique record identifier
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def touch(listener: Option[WriteListener], key: Key)(
    implicit
    policy: WritePolicy = asyncWritePolicyDefault
  ) =
    new AsyncTouch(cluster, policy, listener, key).execute()

  /**
   * Asynchronously determine if a record key exists.
   * This method schedules the exists command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   * <p>
   * The policy can be used to specify timeouts.
   *
   * @param listener				where to send results
   * @param key					unique record identifier
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def exists(listener: Option[ExistsListener], key: Key)(
    implicit
    policy: Policy = asyncReadPolicyDefault
  ) =
    new AsyncExists(cluster, policy, listener, key).execute()

  /**
   * Asynchronously read record generation and expiration only for specified key.  Bins are not read.
   * This method schedules the get command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   * <p>
   * The policy can be used to specify timeouts.
   *
   * @param listener				where to send results
   * @param key					unique record identifier
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def getHeader[A](listener: Option[RecordListener[A]], key: Key)(
    implicit
    policy: Policy = asyncReadPolicyDefault, decoder: Decoder[A]
  ) =
    new AsyncReadHeader[A](cluster, policy, listener, key).execute()

  /**
   * Asynchronously read record header and bins for specified key.
   * This method schedules the get command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   * <p>
   * The policy can be used to specify timeouts.
   *
   * @param listener				where to send results
   * @param key					unique record identifier
   * @param binNames				bins to retrieve
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def get[A](listener: Option[RecordListener[A]], key: Key, binNames: String*)(
    implicit
    policy: Policy = asyncReadPolicyDefault, decoder: Decoder[A]
  ) =
    new AsyncRead(cluster, policy, listener, key, binNames: _*).execute()

  /**
   * Asynchronously perform multiple read/write operations on a single key in one batch call.
   * An example would be to add an integer value to an existing record and then
   * read the result, all in one database call.
   * <p>
   * This method schedules the operate command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   *
   * @param listener				where to send results, pass in null for fire and forget
   * @param key					unique record identifier
   * @param operations			database operations to perform
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def operate[T](listener: Option[RecordListener[T]], key: Key, operations: Operation*)(
    implicit
    policy: WritePolicy = asyncWritePolicyDefault, decoder: Decoder[T]
  ) =
    new AsyncOperate[T](cluster, policy, listener, key, operations.toArray).execute()

  /**
   * Asynchronously read all records in specified namespace and set.  If the policy's
   * <code>concurrentNodes</code> is specified, each server node will be read in
   * parallel.  Otherwise, server nodes are read in series.
   * <p>
   * This method schedules the scan command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   *
   * @param listener				where to send results, pass in null for fire and forget
   * @param namespace				namespace - equivalent to database name
   * @param setName				optional set name - equivalent to database table
   * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
   *                        Aerospike 2 servers ignore this parameter.
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def scanAll[A](listener: Option[RecordSequenceListener[A]], namespace: String, setName: String, binNames: String*)(
    implicit
    policy: ScanPolicy = asyncScanPolicyDefault,
    decoder: Decoder[A]
  ) =
    new AsyncScanExecutor(cluster, policy, listener, namespace, setName, binNames.toArray).execute()

  /**
   * Asynchronously execute user defined function on server and return results.
   * The function operates on a single record.
   * The package name is used to locate the udf file location on the server:
   * <p>
   * udf file = <server udf dir>/<package name>.lua
   * <p>
   * This method schedules the execute command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener.
   *
   * @param listener				where to send results
   * @param key					unique record identifier
   * @param packageName			server package name where user defined function resides
   * @param functionName			user defined function
   * @param functionArgs			arguments passed in to user defined function
   * @throws AerospikeException	if transaction fails
   */
  @throws(classOf[AerospikeException])
  def execute[A, R](listener: Option[ExecuteListener[R]], key: Key, packageName: String, functionName: String, functionArgs: A)(
    implicit
    policy: WritePolicy = asyncWritePolicyDefault,
    encoder: Encoder[A],
    decoder: Decoder[R]
  ) =
    new AsyncExecute[A, R](cluster, policy, listener, key, packageName, functionName, functionArgs).execute()

  //  /**
  //   * Asynchronously execute query on all server nodes.  The query policy's
  //   * <code>maxConcurrentNodes</code> dictate how many nodes can be queried in parallel.
  //   * The default is to query all nodes in parallel.
  //   * <p>
  //   * This method schedules the node's query commands with channel selectors and returns.
  //   * Selector threads will process the commands and send the results to the listener.
  //   *
  //   * @param listener				where to send results
  //   * @param statement				database query command parameters
  //   * @throws AerospikeException	if query fails
  //   */
  //  @throws(classOf[AerospikeException])
  //  def query(listener: RecordSequenceListener, statement: Statement)(
  //    implicit
  //    policy: QueryPolicy = asyncQueryPolicyDefault
  //  ) =
  //    new AsyncQueryExecutor(cluster, policy, listener, statement)

  def close() = client.close()

}
/*
class AsyncBatchCommandExecutor(client: AsyncClient) {
  import client._

  /**
   * Asynchronously check if multiple record keys exist in one batch call.
   * This method schedules the exists command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener in a single call.
   * <p>
   * The policy can be used to specify timeouts and maximum parallel commands.
   *
   * @param listener				where to send results
   * @param keys					array of unique record identifiers
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def exists(listener: ExistsArrayListener, keys: Array[Key])(implicit policy: BatchPolicy = asyncBatchPolicyDefault) =
    if (keys.length == 0)
      listener.onSuccess(keys, new Array[Boolean](0))
    else
      new AsyncBatch.ExistsArrayExecutor(cluster, policy, keys, listener)

  /**
   * Asynchronously read multiple record header data for specified keys in one batch call.
   * This method schedules the get command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener in a single call.
   * <p>
   * If a key is not found, the record will be null.
   * The policy can be used to specify timeouts and maximum parallel commands.
   *
   * @param listener				where to send results
   * @param keys					array of unique record identifiers
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def getHeader[T](listener: RecordArrayListener[T], keys: Array[Key])(
    implicit
    policy: BatchPolicy = asyncBatchPolicyDefault,
    encoder: Encoder[T]
  ) =
    if (keys.length == 0)
      listener.onSuccess(keys, new Array[Record[T]](0))
    else
      ??? //new AsyncBatch.GetArrayExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_NOBINDATA)

  /**
   * Asynchronously read multiple record headers and bins for specified keys in one batch call.
   * This method schedules the get command with a channel selector and returns.
   * Another thread will process the command and send the results to the listener in a single call.
   * <p>
   * If a key is not found, the record will be null.
   * The policy can be used to specify timeouts and maximum parallel commands.
   *
   * @param listener				where to send results
   * @param keys					array of unique record identifiers
   * @param binNames				array of bins to retrieve
   * @throws AerospikeException	if queue is full
   */
  @throws(classOf[AerospikeException])
  def get[T](listener: RecordArrayListener[T], keys: Array[Key], binNames: String*)(
    implicit
    policy: BatchPolicy = asyncBatchPolicyDefault,
    encoder: Encoder[T]
  ) =
    if (binNames.isEmpty)
      if (keys.length == 0)
        listener.onSuccess(keys, new Array[Record[T]](0))
      else
        ??? // new AsyncBatch.GetArrayExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_GET_ALL)
    else if (keys.length == 0)
      listener.onSuccess(keys, new Array[Record[T]](0))
    else
      ??? // new AsyncBatch.GetArrayExecutor(cluster, policy, listener, keys, binNames.toArray, Command.INFO1_READ)

}
*/
