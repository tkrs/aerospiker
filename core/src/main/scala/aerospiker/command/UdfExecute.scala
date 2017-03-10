package aerospiker
package command

import aerospiker.listener.ExecuteListener
import aerospiker.msgpack.JsonPacker
import aerospiker.policy.{ Policy, WritePolicy }
import com.aerospike.client.command.{ FieldType, Buffer => B, Command => C }
import com.aerospike.client.AerospikeException
import com.aerospike.client.async.AsyncCluster
import com.aerospike.client.cluster.Node
import io.circe._
import io.circe.syntax._

final class UdfExecute[A: Encoder, R: Decoder](
    cluster: AsyncCluster,
    writePolicy: WritePolicy,
    listener: Option[ExecuteListener[R]],
    key: Key,
    packageName: String,
    functionName: String,
    args: A
) extends Read[R](cluster, writePolicy, null, key, null) {

  @throws(classOf[AerospikeException])
  override def writeBuffer(): Unit = {
    setUdf(writePolicy, key, packageName, functionName, args)
  }

  override def getNode: Node = cluster.getMasterNode(partition)

  override def onSuccess(): Unit = {
    listener match {
      case None =>
      case Some(l) => l.onSuccess(key, record)
    }
  }

  override def onFailure(e: AerospikeException): Unit = listener match {
    case None =>
    case Some(l) => l.onFailure(e)
  }

  private[this] val packer = JsonPacker()

  @throws(classOf[AerospikeException])
  def setUdf(policy: WritePolicy, key: Key, packageName: String, functionName: String, args: A): Unit = {
    begin()
    var fieldCount: Int = estimateKeySize(policy, key)
    val doc = args.asJson
    val argBytes: Array[Byte] = packer.pack(doc) match {
      case Left(e) => throw e
      case Right(arr) => arr
    }
    fieldCount += estimateUdfSize(packageName, functionName, argBytes)
    sizeBuffer()
    writeHeader(policy, 0, C.INFO2_WRITE, fieldCount, 0)
    writeKey(policy, key)
    writeField(packageName, FieldType.UDF_PACKAGE_NAME)
    writeField(functionName, FieldType.UDF_FUNCTION)
    writeField(argBytes, FieldType.UDF_ARGLIST)
    end()
  }

  private def writeKey(policy: Policy, key: Key): Unit = {
    if (key.namespace != null) {
      writeField(key.namespace, FieldType.NAMESPACE)
    }
    if (key.setName != null) {
      writeField(key.setName, FieldType.TABLE)
    }
    writeField(key.digest, FieldType.DIGEST_RIPE)
    if (policy.sendKey) {
      writeField(key.userKey, FieldType.KEY)
    }
  }

  private def estimateKeySize(policy: Policy, key: Key): Int = {
    var fieldCount: Int = 0
    if (key.namespace != null) {
      dataOffset += B.estimateSizeUtf8(key.namespace) + C.FIELD_HEADER_SIZE
      fieldCount += 1
    }
    if (key.setName != null) {
      dataOffset += B.estimateSizeUtf8(key.setName) + C.FIELD_HEADER_SIZE
      fieldCount += 1
    }
    dataOffset += key.digest.length + C.FIELD_HEADER_SIZE
    fieldCount += 1
    if (policy.sendKey) {
      dataOffset += key.userKey.estimateSize + C.FIELD_HEADER_SIZE + 1
      fieldCount += 1
    }
    fieldCount
  }

  private def estimateUdfSize(packageName: String, functionName: String, bytes: Array[Byte]): Int = {
    dataOffset += B.estimateSizeUtf8(packageName) + C.FIELD_HEADER_SIZE
    dataOffset += B.estimateSizeUtf8(functionName) + C.FIELD_HEADER_SIZE
    dataOffset += bytes.length + C.FIELD_HEADER_SIZE
    3
  }
}
