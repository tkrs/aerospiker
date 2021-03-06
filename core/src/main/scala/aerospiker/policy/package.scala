package aerospiker

import com.aerospike.client.policy.{ CommitLevel, ConsistencyLevel, GenerationPolicy, Priority, RecordExistsAction, Replica }

package object policy {

  implicit val defaultClientPolicy: ClientPolicy = ClientPolicy()

  object ClientPolicy {
    def apply(
      user: String = "",
      password: String = "",
      timeout: Int = 1000,
      maxThreads: Int = 300,
      maxSocketIdle: Int = 14,
      tendInterval: Int = 1000,
      failIfNotConnected: Boolean = true,
      readPolicyDefault: Policy = Policy(),
      writePolicyDefault: WritePolicy = WritePolicy(),
      scanPolicyDefault: ScanPolicy = ScanPolicy(),
      queryPolicyDefault: QueryPolicy = QueryPolicy(),
      batchPolicyDefault: BatchPolicy = BatchPolicy(),
      infoPolicyDefault: InfoPolicy = InfoPolicy(),
      sharedThreadPool: Boolean = false,
      requestProleReplicas: Boolean = false
    ): ClientPolicy = {
      val p = new ClientPolicy()
      p.user = user
      p.password = password
      p.timeout = timeout
      // TODO: lost maxThreads
      // p.maxThreads = maxThreads
      p.maxSocketIdle = maxSocketIdle
      p.tendInterval = tendInterval
      p.failIfNotConnected = failIfNotConnected
      p.asyncReadPolicyDefault = readPolicyDefault
      p.asyncWritePolicyDefault = writePolicyDefault
      p.asyncScanPolicyDefault = scanPolicyDefault
      p.asyncQueryPolicyDefault = queryPolicyDefault
      p.asyncBatchPolicyDefault = batchPolicyDefault
      p.sharedThreadPool = sharedThreadPool
      p.requestProleReplicas = requestProleReplicas
      p
    }
  }
  type ClientPolicy = com.aerospike.client.async.AsyncClientPolicy

  object Policy {
    def apply(
      priority: Priority = Priority.DEFAULT,
      consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
      replica: Replica = Replica.MASTER,
      timeout: Int = 0,
      maxRetries: Int = 1,
      sleepBetweenRetries: Int = 0,
      sendKey: Boolean = false
    ): Policy = {
      val p: Policy = new Policy()
      p.priority = priority
      p.consistencyLevel = consistencyLevel
      p.replica = replica
      p.timeout = timeout
      p.maxRetries = maxRetries
      p.sleepBetweenRetries = sleepBetweenRetries
      p.sendKey = sendKey
      p
    }
  }
  type Policy = com.aerospike.client.policy.Policy

  object WritePolicy {
    def apply(
      priority: Priority = Priority.DEFAULT,
      consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
      replica: Replica = Replica.MASTER,
      timeout: Int = 0,
      maxRetries: Int = 1,
      sleepBetweenRetries: Int = 0,
      sendKey: Boolean = false,
      recordExistsAction: RecordExistsAction = RecordExistsAction.UPDATE,
      generationPolicy: GenerationPolicy = GenerationPolicy.NONE,
      commitLevel: CommitLevel = CommitLevel.COMMIT_ALL,
      generation: Int = 0,
      expiration: Int = 0
    ): WritePolicy = {
      val p = new WritePolicy()
      p.priority = priority
      p.consistencyLevel = consistencyLevel
      p.replica = replica
      p.timeout = timeout
      p.maxRetries = maxRetries
      p.sleepBetweenRetries = sleepBetweenRetries
      p.sendKey = sendKey
      p.recordExistsAction = recordExistsAction
      p.generationPolicy = generationPolicy
      p.commitLevel = commitLevel
      p.generation = generation
      p.expiration = expiration
      p
    }
  }
  type WritePolicy = com.aerospike.client.policy.WritePolicy

  object ScanPolicy {
    def apply(
      priority: Priority = Priority.DEFAULT,
      consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
      replica: Replica = Replica.MASTER,
      timeout: Int = 0,
      maxRetries: Int = 0,
      sleepBetweenRetries: Int = 0,
      sendKey: Boolean = false,
      scanPercent: Int = 100,
      maxConcurrentNodes: Int = 0,
      concurrentNodes: Boolean = true,
      includeBinData: Boolean = true,
      failOnClusterChange: Boolean = false
    ): ScanPolicy = {
      val p = new ScanPolicy()
      p.priority = priority
      p.consistencyLevel = consistencyLevel
      p.replica = replica
      p.timeout = timeout
      p.maxRetries = maxRetries
      p.sleepBetweenRetries = sleepBetweenRetries
      p.sendKey = sendKey
      p.scanPercent = scanPercent
      p.maxConcurrentNodes = maxConcurrentNodes
      p.concurrentNodes = concurrentNodes
      p.includeBinData = includeBinData
      p.failOnClusterChange = failOnClusterChange
      p
    }
  }
  type ScanPolicy = com.aerospike.client.policy.ScanPolicy

  object QueryPolicy {
    def apply(
      priority: Priority = Priority.DEFAULT,
      consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
      replica: Replica = Replica.MASTER,
      timeout: Int = 0,
      maxRetries: Int = 0,
      sleepBetweenRetries: Int = 0,
      sendKey: Boolean = false,
      maxConcurrentNodes: Int = 0,
      recordQueueSize: Int = 5000
    ): QueryPolicy = {
      val p = new QueryPolicy()
      p.priority = priority
      p.consistencyLevel = consistencyLevel
      p.replica = replica
      p.timeout = timeout
      p.maxRetries = maxRetries
      p.sleepBetweenRetries = sleepBetweenRetries
      p.sendKey = sendKey
      p.maxConcurrentNodes = maxConcurrentNodes
      p.recordQueueSize = recordQueueSize
      p
    }
  }
  type QueryPolicy = com.aerospike.client.policy.QueryPolicy

  object BatchPolicy {
    def apply(
      priority: Priority = Priority.DEFAULT,
      consistencyLevel: ConsistencyLevel = ConsistencyLevel.CONSISTENCY_ONE,
      replica: Replica = Replica.MASTER,
      timeout: Int = 0,
      maxRetries: Int = 1,
      sleepBetweenRetries: Int = 0,
      sendKey: Boolean = false,
      maxConcurrentThreads: Int = 1,
      useBatchDirect: Boolean = false,
      allowInline: Boolean = true
    ): BatchPolicy = {
      val p = new BatchPolicy()
      p.priority = priority
      p.consistencyLevel = consistencyLevel
      p.replica = replica
      p.timeout = timeout
      p.maxRetries = maxRetries
      p.sleepBetweenRetries = sleepBetweenRetries
      p.sendKey = sendKey
      p.maxConcurrentThreads = maxConcurrentThreads
      p.useBatchDirect = useBatchDirect
      p.allowInline = allowInline
      p
    }
  }
  type BatchPolicy = com.aerospike.client.policy.BatchPolicy

  object InfoPolicy {
    def apply(timeout: Int = 1000): InfoPolicy = {
      val p = new InfoPolicy()
      p.timeout = timeout
      p
    }
  }
  type InfoPolicy = com.aerospike.client.policy.InfoPolicy
}
