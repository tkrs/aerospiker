package aerospiker
package command

// TODO: hate!

import aerospiker.data._
import aerospiker.buffer.Buffer
import aerospiker.listener.RecordSequenceListener
import aerospiker.policy.{ Policy, ScanPolicy }
import cats.data.Xor._
import com.aerospike.client.AerospikeException
import com.aerospike.client.AerospikeException.Parse
import com.aerospike.client.async.{ AsyncNode, AsyncCluster, AsyncMultiExecutor, AsyncMultiCommand }
import io.circe._
import scala.collection.mutable.ListBuffer

final class Scan[T](
    parent: AsyncMultiExecutor,
    cluster: AsyncCluster,
    node: AsyncNode,
    policy: ScanPolicy,
    listener: Option[RecordSequenceListener[T]],
    namespace: String,
    setName: String,
    binNames: Array[String],
    taskId: Long
)(
    implicit
    decoder: Decoder[T]
) extends AsyncMultiCommand(parent, cluster, node, true) {

  def getPolicy: Policy = policy

  @throws(classOf[AerospikeException])
  def writeBuffer(): Unit = setScan(policy, namespace, setName, binNames, taskId)

  @throws(classOf[AerospikeException])
  def parseRow(key: Key): Unit = {
    val record: Option[Record[T]] = parseRecord0()
    listener match {
      case Some(l) => l.onRecord(key, record)
      case None => // nop
    }
  }

  @throws(classOf[AerospikeException])
  def parseRecord0(): Option[Record[T]] = {
    val bins: ListBuffer[(String, Json)] = ListBuffer.empty
    for (i <- 0 until opCount) {
      val opSize: Int = Buffer.bytesToInt(receiveBuffer.slice(receiveOffset, receiveOffset + 4))
      val particleType: Byte = receiveBuffer(receiveOffset + 5)
      val nameSize: Byte = receiveBuffer(receiveOffset + 7)
      val name: String = new String(receiveBuffer.slice(receiveOffset + 8, receiveOffset + 8 + nameSize))
      receiveOffset += 4 + 4 + nameSize
      val particleBytesSize: Int = opSize - (4 + nameSize)
      val value: Json = Buffer.bytesToParticle(particleType.toInt, receiveBuffer.slice(receiveOffset, receiveOffset + particleBytesSize))
      receiveOffset += particleBytesSize

      bins += name -> value
    }
    val json = Json.obj(bins: _*)
    json.as[T] match {
      case Left(e) => throw new Parse(e.getMessage)
      case Right(t) => Some(Record[T](Some(t), generation, expiration))
    }

  }
}

