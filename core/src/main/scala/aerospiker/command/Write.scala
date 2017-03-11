package aerospiker
package command

import java.nio.ByteBuffer

import aerospiker.listener.WriteListener
import aerospiker.policy.{ Policy, WritePolicy }
import com.aerospike.client.async.{ AsyncCluster, AsyncCommand, AsyncSingleCommand }
import com.aerospike.client.cluster.{ Node, Partition }
import com.aerospike.client.{ AerospikeException, Operation }
import io.circe.Encoder

final class Write[T: Encoder](
    cluster: AsyncCluster,
    policy: WritePolicy,
    listener: Option[WriteListener],
    key: Key,
    bins: T,
    operation: Operation.Type
) extends AsyncSingleCommand(cluster, policy) {

  val partition = new Partition(key)

  def getPolicy: Policy = policy

  def writeBuffer(): Unit = {
    val buffer = protocol.setWrite(policy, operation, key, bins)
    sizeBuffer()
    System.arraycopy(buffer, 0, dataBuffer, 0, buffer.length)
    dataOffset = buffer.length
  }

  def getNode: Node = cluster.getMasterNode(partition)

  def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    if (resultCode != 0) throw new AerospikeException(resultCode)
  }

  def onSuccess(): Unit = listener.foreach(_.onSuccess(key))

  def onFailure(e: AerospikeException): Unit = listener.foreach(_.onFailure(e))

  override def cloneCommand(): AsyncCommand = new Write[T](cluster, policy, listener, key, bins, operation)
}