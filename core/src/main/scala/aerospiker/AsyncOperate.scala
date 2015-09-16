package aerospiker

import com.aerospike.client.Operation
import com.aerospike.client.async.{ AsyncCluster, AsyncNode }
import com.aerospike.client.policy.WritePolicy
import io.circe.Decoder

final class AsyncOperate[A](
    cluster: AsyncCluster,
    policy: WritePolicy,
    listener: Option[RecordListener[A]],
    key: Key, operations: Array[Operation]
)(
    implicit
    decoder: Decoder[A]
) extends AsyncRead[A](cluster, policy, listener, key) {
  override def writeBuffer(): Unit = setOperate(policy, key, operations)
  override def getNode: AsyncNode = cluster.getMasterNode(partition).asInstanceOf[AsyncNode]
}

