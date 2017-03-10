package aerospiker
package buffer

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class Header(
    headerLength: Int = 0,
    readAttr: Int = 0,
    writeAttr: Int = 0,
    infoAttr: Int = 0,
    resultCode: Int = 0,
    generation: Int = 0,
    expiration: Int = 0,
    timeOut: Int = 0,
    fieldCount: Int = 0,
    operationCount: Int = 0
) {
  def withInfoAttr(infoAttr: Int): Header = copy(infoAttr = infoAttr)
  def withReadAttr(readAttr: Int): Header = copy(readAttr = readAttr)
  def withWriteAttr(writeAttr: Int): Header = copy(writeAttr = writeAttr)
  def withGeneration(generation: Int): Header = copy(generation = generation)
  def getBytes: Seq[Byte] = {
    // Write all header data except total size which must be written last.
    val bytes: mutable.ArrayBuilder[Byte] = mutable.ArrayBuilder.make()
    bytes += headerLength.toByte // Message header length.
    bytes += readAttr.toByte
    bytes += writeAttr.toByte
    bytes += infoAttr.toByte
    bytes += 0.toByte //
    bytes += 0.toByte // clear the result code
    bytes ++= Buffer.intToBytes(generation)
    bytes ++= Buffer.intToBytes(expiration)
    // Initialize timeout. It will be written later.
    bytes ++= (0 :: 0 :: 0 :: 0 :: Nil).map(_.toByte)
    bytes ++= Buffer.shortToBytes(fieldCount.toShort)
    bytes ++= Buffer.shortToBytes(operationCount.toShort)
    bytes.result()
  }
}
