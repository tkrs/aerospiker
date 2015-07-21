package org.aerospiker

import com.aerospike.client.policy.{
  Priority,
  ConsistencyLevel,
  Replica,
  RecordExistsAction,
  GenerationPolicy,
  CommitLevel
}

object policy {

  final case class Host(
    name: String = "127.0.0.1",
    port: Int = 3000)

  final case class ClientPolicy(
    user: String = "",
    pwd: String = "",
    maxThreads: Int = 300,
    maxSocketIdle: Int = 14,
    tendInterval: Int = 1000,
    failIfNotConnected: Boolean = true,
    sharedThreadPool: Boolean = false,
    requestProleReplicas: Boolean = false)

  final case class ReadPolicy(
    priority: Priority = Priority.DEFAULT,
    consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
    replica: Replica = Replica.MASTER,
    timeout: Int = 0,
    maxRetries: Int = 1,
    sleepBetweenRetries: Int = 500,
    sendKey: Boolean = false)

  final case class WritePolicy(
    priority: Priority = Priority.DEFAULT,
    consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
    replica: Replica = Replica.MASTER,
    timeout: Int = 0,
    maxRetries: Int = 1,
    sleepBetweenRetries: Int = 500,
    sendKey: Boolean = false,
    recordExistsAction: RecordExistsAction = RecordExistsAction.UPDATE,
    generationPolicy: GenerationPolicy = GenerationPolicy.NONE,
    commitLevel: CommitLevel = CommitLevel.COMMIT_ALL,
    generation: Int = 0,
    expiration: Int = 0)

  final case class ScanPolicy(
    priority: Priority = Priority.DEFAULT,
    consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
    replica: Replica = Replica.MASTER,
    timeout: Int = 0,
    maxRetries: Int = 0,
    sleepBetweenRetries: Int = 500,
    sendKey: Boolean = false,
    scanPercent: Int = 100,
    maxConcurrentNodes: Int = 0,
    concurrentNodes: Boolean = true,
    includeBinData: Boolean = true,
    failOnClusterChange: Boolean = false)

  final case class QueryPolicy(
    priority: Priority = Priority.DEFAULT,
    consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
    replica: Replica = Replica.MASTER,
    timeout: Int = 0,
    maxRetries: Int = 0,
    sleepBetweenRetries: Int = 500,
    sendKey: Boolean = false,
    recordQueueSize: Int = 5000)

  final case class BatchPolicy(
    priority: Priority = Priority.DEFAULT,
    consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
    replica: Replica = Replica.MASTER,
    timeout: Int = 0,
    maxRetries: Int = 0,
    sleepBetweenRetries: Int = 500,
    sendKey: Boolean = false,
    maxConcurrentThreads: Int = 1,
    useBatchDirect: Boolean = false,
    allowInline: Boolean = true)

  final case class InfoPolicy(
    priority: Priority = Priority.DEFAULT,
    consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
    replica: Replica = Replica.MASTER,
    timeout: Int = 1000,
    maxRetries: Int = 0,
    sleepBetweenRetries: Int = 500,
    sendKey: Boolean = false)
}
