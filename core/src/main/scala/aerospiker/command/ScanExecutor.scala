package aerospiker
package command

import aerospiker.listener.RecordSequenceListener
import com.aerospike.client.AerospikeException
import com.aerospike.client.ResultCode
import com.aerospike.client.async.{ AsyncCluster, AsyncNode, AsyncMultiExecutor }
import com.aerospike.client.cluster.Node

import aerospiker.policy.ScanPolicy
import io.circe.Decoder

import scala.collection.mutable.ListBuffer

final class ScanExecutor[A](
    cluster: AsyncCluster,
    policy: ScanPolicy,
    listener: Option[RecordSequenceListener[A]],
    namespace: String,
    setName: String,
    binNames: Array[String]
)(
    implicit
    decoder: Decoder[A]
) extends AsyncMultiExecutor {
  val nodes: Array[Node] = cluster.getNodes
  if (nodes.length == 0) {
    throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
  }
  val taskId: Long = System.nanoTime
  var count: Int = 0

  def execute(): Unit = {
    val tasks: ListBuffer[Scan[A]] = ListBuffer.empty
    for (node <- nodes) {
      tasks += new Scan[A](this, cluster, node.asInstanceOf[AsyncNode], policy, listener, namespace, setName, binNames, taskId)
    }
    execute(tasks.toArray, policy.maxConcurrentNodes)
  }

  def onSuccess(): Unit = listener match {
    case Some(l) => l.onSuccess()
    case None => // nop
  }

  def onFailure(ae: AerospikeException): Unit = listener match {
    case Some(l) => l.onFailure(ae)
    case None => // nop
  }
}

