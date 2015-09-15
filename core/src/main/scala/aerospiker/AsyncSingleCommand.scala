package aerospiker

import java.io.IOException
import java.nio.ByteBuffer

import com.aerospike.client.AerospikeException
import com.aerospike.client.async.AsyncCluster

abstract class AsyncSingleCommand(cluster: AsyncCluster) extends com.aerospike.client.async.AsyncCommand(cluster) {
  var receiveSize: Int = 0

  @throws(classOf[AerospikeException])
  @throws(classOf[IOException])
  def read(): Unit = {
    if (inHeader) {
      if (!conn.read(byteBuffer)) {
        return
      }
      byteBuffer.position(0)
      receiveSize = (byteBuffer.getLong & 0xFFFFFFFFFFFFL).toInt
      if (receiveSize <= byteBuffer.capacity) {
        byteBuffer.clear
        byteBuffer.limit(receiveSize)
      } else {
        byteBuffer = ByteBuffer.allocateDirect(receiveSize)
      }
      inHeader = false
    }
    if (!conn.read(byteBuffer)) {
      return
    }
    if (inAuthenticate) {
      processAuthenticate()
      return
    }
    parseResult(byteBuffer)
    finish()
  }

  @throws(classOf[AerospikeException])
  def parseResult(byteBuffer: ByteBuffer): Unit
}
