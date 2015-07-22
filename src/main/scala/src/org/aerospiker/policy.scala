package org.aerospiker

object policy {
  type ClientPolicy = com.aerospike.client.policy.ClientPolicy
  type Policy = com.aerospike.client.policy.Policy
  type WritePolicy = com.aerospike.client.policy.WritePolicy
  type ScanPolicy = com.aerospike.client.policy.ScanPolicy
  type QueryPolicy = com.aerospike.client.policy.QueryPolicy
  type BatchPolicy = com.aerospike.client.policy.BatchPolicy
  type InfoPolicy = com.aerospike.client.policy.InfoPolicy
}
