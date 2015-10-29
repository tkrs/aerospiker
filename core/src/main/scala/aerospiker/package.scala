import aerospiker.buffer.Buffer
import cats.{ FlatMap, Functor }
import cats.data.ReaderT

import scala.collection.mutable.ListBuffer
import scalaz.concurrent.Task

package object aerospiker {

  type Action[U] = ReaderT[Task, AerospikeClient, U]

  object Command extends Functions

  implicit val TaskFunctor: Functor[Task] = new Functor[Task] {
    override def map[A, B](fa: Task[A])(f: (A) => B): Task[B] = fa.map(f)
  }

  implicit val TaskFlatMap: FlatMap[Task] = new FlatMap[Task] {
    override def flatMap[A, B](fa: Task[A])(f: (A) => Task[B]): Task[B] = fa.flatMap(f)
    override def map[A, B](fa: Task[A])(f: (A) => B): Task[B] = fa.map(f)
  }

  type Host = com.aerospike.client.Host
  type Key = com.aerospike.client.Key
  type Bin = com.aerospike.client.Bin
  type Value = com.aerospike.client.Value

  object Host {
    def apply(name: String, port: Int) = new Host(name, port)
  }

  object Key {
    def apply(namespace: String, set: String, key: String) = new Key(namespace, set, key)
  }

  object data {
    case class Record[T](bins: Option[T], generation: Int, expiration: Int)
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
      def withInfoAttr(infoAttr: Int) = copy(infoAttr = infoAttr)
      def withReadAttr(readAttr: Int) = copy(readAttr = readAttr)
      def withWriteAttr(writeAttr: Int) = copy(writeAttr = writeAttr)
      def withGeneration(generation: Int) = copy(generation = generation)
      def getBytes: Seq[Byte] = {
        // Write all header data except total size which must be written last.
        val bytes: ListBuffer[Byte] = ListBuffer.empty
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
        bytes.toSeq
      }
    }
  }

}

