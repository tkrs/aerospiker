package aerospiker

import java.nio.ByteBuffer
import com.aerospike.client.{ AerospikeException, ResultCode }
import com.aerospike.client.async.{ AsyncCluster, AsyncNode, AsyncSingleCommand }
import com.aerospike.client.cluster.Partition
import com.aerospike.client.policy.Policy
import com.typesafe.scalalogging.LazyLogging

final class AsyncExists(cluster: AsyncCluster, policy: Policy, listener: Option[ExistsListener], key: Key) extends AsyncSingleCommand(cluster) with LazyLogging {

  private val partition: Partition = new Partition(key)

  private var exists = false

  def getPolicy: Policy = policy

  def writeBuffer: Unit = setExists(policy, key)

  def getNode: AsyncNode = {
    cluster.getReadNode(partition, policy.replica).asInstanceOf[AsyncNode]
  }

  def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    if (resultCode == 0)
      exists = true
    else if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) exists = false
    else throw new AerospikeException(resultCode)
  }

  def onSuccess: Unit = {
    listener match {
      case Some(l) => l.onSuccess(key, exists)
      case None => // nop
    }
  }

  def onFailure(e: AerospikeException): Unit = {
    listener match {
      case Some(l) => l.onFailure(e)
      case None => // nop
    }
  }
}
