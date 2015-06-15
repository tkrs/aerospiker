package org.aerospiker

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import com.aerospike.client.{ Host => AsHost, Bin => AsBin, Key => AsKey, Record => AsRecord, AerospikeClient, AerospikeException }
import com.aerospike.client.policy.ClientPolicy
import scalaz._

import Conversions._

object Client {
  def apply(settings: Settings): Client = new Client(settings)
}

class Client(settings: Settings)
  extends BaseClient(settings)
  with Operation

class BaseClient(settings: Settings) {

  val asClient: AerospikeClient = {

    val policy = new ClientPolicy();
    policy.user = settings.user
    policy.password = settings.pwd
    policy.readPolicyDefault.maxRetries = settings.maxRetries
    policy.writePolicyDefault.maxRetries = settings.maxRetries
    policy.failIfNotConnected = true;

    new AerospikeClient(policy, settings.host.trans: _*)
  }

  def close(): Unit = {
    asClient.close()
  }

}
