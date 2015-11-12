package aerospiker
package command

import java.nio.ByteBuffer

import com.aerospike.client.{ ResultCode, AerospikeException }
import com.aerospike.client.async.{ AsyncNode, AsyncCluster, AsyncSingleCommand }
import com.aerospike.client.cluster.Partition

import policy.{ Policy, WritePolicy }
import listener.DeleteListener

final class Delete(cluster: AsyncCluster, policy: WritePolicy, listener: Option[DeleteListener], key: Key) extends AsyncSingleCommand(cluster) {

  private val partition: Partition = new Partition(key)
  private var existed: Boolean = false

  def getPolicy: Policy = policy

  def writeBuffer(): Unit = setDelete(policy, key)

  def getNode: AsyncNode = cluster.getMasterNode(partition).asInstanceOf[AsyncNode]

  def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    if (resultCode == 0) {
      existed = true
    } else {
      if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
        existed = false
      } else {
        throw new AerospikeException(resultCode)
      }
    }
  }

  def onSuccess(): Unit = listener match {
    case Some(l) => l.onSuccess(key, existed)
    case None => // nop
  }

  def onFailure(e: AerospikeException): Unit = listener match {
    case Some(l) => l.onFailure(e)
    case None => // nop
  }
}

