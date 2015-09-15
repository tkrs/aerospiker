package aerospiker

import java.util.concurrent.atomic.AtomicBoolean

import com.aerospike.client.AerospikeException
import com.aerospike.client.admin.AdminCommand
import com.aerospike.client.async.{ AsyncCluster, AsyncConnection, AsyncNode }
import com.aerospike.client.policy.Policy
import com.aerospike.client.util.ThreadLocalData
import com.typesafe.scalalogging.LazyLogging

abstract class AsyncCommand(cluster: AsyncCluster) extends com.aerospike.client.async.AsyncCommand(cluster) with LazyLogging {

  val complete = new AtomicBoolean()
  var limit = 0L
  var failedNodes = 0
  var failedConns = 0

  @throws(classOf[AerospikeException])
  override def execute(): Unit = {
    val policy: Policy = getPolicy
    timeout = policy.timeout
    if (timeout > 0) {
      limit = System.currentTimeMillis + timeout
    }
    byteBuffer = cluster.getByteBuffer
    executeCommand()
  }

  override def executeCommand(): Unit = {
    if (complete.get()) return

    try {
      node = getNode()
      conn = node.getAsyncConnection()

      if (conn == null) {
        conn = new AsyncConnection(node.getAddress(), cluster)
        logger.debug(conn.isValid.toString)

        if (cluster.getUser() != null) {
          inAuthenticate = true
          dataBuffer = ThreadLocalData.getBuffer()
          val command = new AdminCommand(dataBuffer)
          dataOffset = command.setAuthenticate(cluster.getUser(), cluster.getPassword())
          byteBuffer.clear()
          byteBuffer.put(dataBuffer, 0, dataOffset)
          byteBuffer.flip()
          conn.execute(this)
          return
        }
      }
      writeCommand()
      conn.execute(this)
    } catch {
      case ai: AerospikeException.InvalidNode =>
        failedNodes += 1
        if (!retryOnInit()) throw ai
      case ce: AerospikeException.Connection =>
        // Socket connection error has occurred.
        failedConns += 1
        if (!retryOnInit()) throw ce
      case e: Exception =>
        if (!failOnApplicationInit()) {
          throw new AerospikeException(e)
        }
    }
  }

  def writeBuffer(): Unit
  def getPolicy: Policy
  def getNode: AsyncNode
  def onFailure(ae: AerospikeException): Unit
  def onSuccess(): Unit
  def read(): Unit

  private[this] def retryOnInit(): Boolean = {
    if (complete.get()) {
      return true
    }

    val policy = getPolicy()

    iteration += 1
    if (iteration > policy.maxRetries) {
      return failOnNetworkInit()
    }

    // Disable sleep between retries.  Sleeping in asynchronous mode makes no sense.
    // if (limit > 0 && System.currentTimeMillis() + policy.sleepBetweenRetries > limit) {

    if (limit > 0 && System.currentTimeMillis() > limit) {
      // Might as well stop here because the transaction will
      // timeout after sleep completed.
      return failOnNetworkInit()
    }

    // Prepare for retry.
    resetConnection()

    // Disable sleep between retries.  Sleeping in asynchronous mode makes no sense.

    // Retry command recursively.
    executeCommand()
    true
  }

  private[this] def resetConnection(): Unit = {
    if (limit > 0) {
      // A lock on reset is required when a client timeout is specified.
      this synchronized {
        if (conn != null) {
          conn.close()
          conn = null
        }
      }
    } else {
      if (conn != null) {
        conn.close()
        conn = null
      }
    }
  }
  private[this] def failOnNetworkInit(): Boolean = {
    // Ensure that command succeeds or fails, but not both.
    if (complete.compareAndSet(false, true)) {
      closeOnNetworkError()
      false
    } else {
      true
    }
  }

  private[this] def failOnApplicationInit(): Boolean = {
    // Ensure that command succeeds or fails, but not both.
    if (complete.compareAndSet(false, true)) {
      close()
      false
    } else {
      true
    }
  }

  private[this] def failOnNetworkError(ae: AerospikeException): Unit = {
    // Ensure that command succeeds or fails, but not both.
    if (complete.compareAndSet(false, true)) {
      closeOnNetworkError()
      onFailure(ae)
    }
  }

  private[this] def failOnClientTimeout(): Unit = {
    // Free up resources and notify.
    closeOnNetworkError()
    onFailure(new AerospikeException.Timeout(node, timeout, iteration, failedNodes, failedConns))
  }

  private[this] def closeOnNetworkError(): Unit = close()

  private[this] def close(): Unit = {
    if (conn != null) {
      conn.close()
    }
    cluster.putByteBuffer(byteBuffer)
  }

}
