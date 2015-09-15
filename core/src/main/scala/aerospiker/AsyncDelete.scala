/*
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package aerospiker

import java.nio.ByteBuffer

import com.aerospike.client.{ ResultCode, AerospikeException }
import com.aerospike.client.async.{ AsyncNode, AsyncCluster, AsyncSingleCommand }
import com.aerospike.client.cluster.Partition
import com.typesafe.scalalogging.LazyLogging

import policy.{ Policy, WritePolicy }

final class AsyncDelete(cluster: AsyncCluster, policy: WritePolicy, listener: Option[DeleteListener], key: Key) extends AsyncSingleCommand(cluster) with LazyLogging {

  private val partition: Partition = new Partition(key)
  private var existed: Boolean = false

  def getPolicy: Policy = policy

  def writeBuffer(): Unit = setDelete(policy, key)

  def getNode: AsyncNode = cluster.getMasterNode(partition).asInstanceOf[AsyncNode]

  def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    if (resultCode == 0) {
      existed = true
    } else {
      if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
        existed = false
      } else {
        throw new AerospikeException(resultCode)
      }
    }
  }

  def onSuccess(): Unit = listener match {
    case Some(l) => l.onSuccess(key, existed)
    case None => // nop
  }

  def onFailure(e: AerospikeException): Unit = listener match {
    case Some(l) => l.onFailure(e)
    case None => // nop
  }
}

