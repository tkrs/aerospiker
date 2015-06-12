package org.aerospiker

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest._

class OperationSpec extends FlatSpec with Matchers {

  "A Operation" should "put values" in {
    // val settings = Settings(host = "172.17.0.28:3000")
    val hosts = Array(
      sys.env("AEROSPIKE_SERVER_PORT_3000_TCP_ADDR") + ":" + sys.env("AEROSPIKE_SERVER_PORT_3000_TCP_PORT"),
      sys.env("AEROSPIKE_SERVER_PORT_3001_TCP_ADDR") + ":" + sys.env("AEROSPIKE_SERVER_PORT_3001_TCP_PORT"),
      sys.env("AEROSPIKE_SERVER_PORT_3002_TCP_ADDR") + ":" + sys.env("AEROSPIKE_SERVER_PORT_3002_TCP_PORT"),
      sys.env("AEROSPIKE_SERVER_PORT_3003_TCP_ADDR") + ":" + sys.env("AEROSPIKE_SERVER_PORT_3003_TCP_PORT")
    )
    // TODO: multi host...
    val settings = Settings(host = hosts(0))

    val client = Client(settings)
    val key = new Key("test", "teste", "testee")
    val bin = new Bin("nickname", new Value("tkrs"))
    val f = client.put(key, bin).run
    f onSuccess {
      case msg => println("string put done")
    }
    Await.result(f, Duration(100, "millis"))
  }

  // it should "throw NoSuchElementException if an empty stack is popped" in {
  //   val emptyStack = new Stack[Int]
  //   a[NoSuchElementException] should be thrownBy {
  //     emptyStack.pop()
  //   }
  // }
}
