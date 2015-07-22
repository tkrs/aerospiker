package org.aerospiker

import com.aerospike.client.{
  Host => AsHost,
  Key => AsKey,
  Bin => AsBin,
  Value => AsValue,
  Record => AsRecord
}
import com.aerospike.client.policy.{
  ClientPolicy => AsClientPolicy,
  Policy => AsPolicy,
  WritePolicy => AsWritePolicy,
  ScanPolicy => AsScanPolicy,
  QueryPolicy => AsQueryPolicy,
  BatchPolicy => AsBatchPolicy,
  InfoPolicy => AsInfoPolicy
}
import java.util.{ Map => JMap, List => JList }
import java.lang.{
  Integer => JInteger,
  Long => JLong,
  Double => JDouble,
  Float => JFloat,
  Boolean => JBool
}

import scala.collection.mutable.{ Buffer => MBuffer, Map => MMap }
import scala.collection.JavaConversions._
import policy._

object Conversions {

  implicit def toAsClientPolicy(p: ClientPolicy)(
    implicit readPolicy: ReadPolicy = ReadPolicy(),
    writePolicy: WritePolicy = WritePolicy(),
    scanPolicy: ScanPolicy = ScanPolicy(),
    queryPolicy: QueryPolicy = QueryPolicy(),
    batchPolicy: BatchPolicy = BatchPolicy(),
    infoPolicy: InfoPolicy = InfoPolicy()): AsClientPolicy = {

    val x = new AsClientPolicy()
    x.user = p.user
    x.password = p.pwd
    x.maxThreads = p.maxThreads
    x.maxSocketIdle = p.maxSocketIdle
    x.tendInterval = p.tendInterval
    x.failIfNotConnected = p.failIfNotConnected
    x.sharedThreadPool = p.sharedThreadPool
    x.requestProleReplicas = p.requestProleReplicas

    x.readPolicyDefault = readPolicy
    x.writePolicyDefault = writePolicy
    x.scanPolicyDefault = scanPolicy
    x.queryPolicyDefault = queryPolicy
    x.batchPolicyDefault = batchPolicy
    x.infoPolicyDefault = infoPolicy
    x
  }

  implicit def toAsPolicy(p: ReadPolicy): AsPolicy = {
    val x = new AsPolicy()
    x.priority = p.priority
    x.consistencyLevel = p.consistencyLevel
    x.replica = p.replica
    x.timeout = p.timeout
    x.maxRetries = p.maxRetries
    x.sleepBetweenRetries = p.sleepBetweenRetries
    x.sendKey = p.sendKey
    x
  }

  implicit def toAsWritePolicy(p: WritePolicy): AsWritePolicy = {
    val x = new AsWritePolicy()
    x.recordExistsAction = p.recordExistsAction
    x.generationPolicy = p.generationPolicy
    x.commitLevel = p.commitLevel
    x.generation = p.generation
    x.expiration = p.expiration

    x.priority = p.priority
    x.consistencyLevel = p.consistencyLevel
    x.replica = p.replica
    x.timeout = p.timeout
    x.maxRetries = p.maxRetries
    x.sleepBetweenRetries = p.sleepBetweenRetries
    x.sendKey = p.sendKey
    x
  }

  implicit def toScanPolicy(p: ScanPolicy): AsScanPolicy = {
    val x = new AsScanPolicy()
    x.scanPercent = p.scanPercent
    x.maxConcurrentNodes = p.maxConcurrentNodes
    x.concurrentNodes = p.concurrentNodes
    x.includeBinData = p.includeBinData
    x.failOnClusterChange = p.failOnClusterChange

    x.priority = p.priority
    x.consistencyLevel = p.consistencyLevel
    x.replica = p.replica
    x.timeout = p.timeout
    x.maxRetries = p.maxRetries
    x.sleepBetweenRetries = p.sleepBetweenRetries
    x.sendKey = p.sendKey
    x
  }

  implicit def toAsQueryPolicy(p: QueryPolicy): AsQueryPolicy = {
    val x = new AsQueryPolicy()
    x.recordQueueSize = p.recordQueueSize

    x.priority = p.priority
    x.consistencyLevel = p.consistencyLevel
    x.replica = p.replica
    x.timeout = p.timeout
    x.maxRetries = p.maxRetries
    x.sleepBetweenRetries = p.sleepBetweenRetries
    x.sendKey = p.sendKey
    x
  }

  implicit def toAsBatchPolicy(p: BatchPolicy): AsBatchPolicy = {
    val x = new AsBatchPolicy()
    x.maxConcurrentThreads = p.maxConcurrentThreads
    x.useBatchDirect = p.useBatchDirect
    x.allowInline = p.allowInline

    x.priority = p.priority
    x.consistencyLevel = p.consistencyLevel
    x.replica = p.replica
    x.timeout = p.timeout
    x.maxRetries = p.maxRetries
    x.sleepBetweenRetries = p.sleepBetweenRetries
    x.sendKey = p.sendKey
    x
  }

  implicit def toAsInfoPolicy(p: InfoPolicy): AsInfoPolicy = {
    val x = new AsInfoPolicy()
    x.timeout = p.timeout
    x
  }

  implicit class HostsConversion(x: Seq[Host]) {
    def toAsHosts: Seq[AsHost] = x map { h => new AsHost(h.name, h.port) }
  }

  implicit class AsRecordConversion(x: AsRecord) {
    def toRecordOption: Option[Record] = {
      def convert(v: Any): Any = v match {
        case x: JBool => x: Boolean
        case x: JList[_] => { x: MBuffer[_] } map { convert(_) }
        case x: JMap[String, _] => { x: MMap[String, _] } mapValues { convert(_) }
        case x: JFloat => x: Float
        case x: JDouble => x: Double
        case x: JInteger => x: Int
        case x: JLong => x: Long
        case x => x
      }

      for {
        rec <- Option(x)
        b <- Option(rec.bins)
        r <- Some(Record(
          bins = b.mapValues(convert(_)) toMap,
          generation = rec.generation,
          expiration = rec.expiration))
      } yield r

    }
  }

  implicit class KeyConversion(x: Key) {
    def toAsKey: AsKey = {
      new AsKey(x.namespace, x.set, x.key)
    }
  }

  implicit class ValueToAsValue(x: Value[_]) {
    def toAsValue: AsValue = {
      def _toAsValue(v: Any): AsValue = v match {
        case v: Int => new AsValue.IntegerValue(v)
        case v: String => new AsValue.StringValue(v)
        case v: Array[Byte] => new AsValue.BytesValue(v)
        case v: Long => new AsValue.LongValue(v)
        case v: List[_] => new AsValue.ListValue(v map { _toAsValue(_).getObject() })
        case v: Map[_, _] => new AsValue.MapValue(v mapValues { _toAsValue(_).getObject() })
        case null => new AsValue.NullValue()
        case v => new AsValue.BlobValue(v)
      }
      _toAsValue(x)
    }
  }

  implicit class BinConversion(x: Bin[_]) {
    def toAsBin: AsBin = {
      new AsBin(x.name, x.value.value)
    }
  }

}
