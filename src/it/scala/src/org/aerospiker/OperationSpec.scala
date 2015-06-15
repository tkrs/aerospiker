package org.aerospiker

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

import scalaz._

import org.scalatest._
import org.scalatest.Assertions._

class OperationSpec extends FlatSpec with Matchers {

  val hosts = {
    (
      ("AEROSPIKE_SERVER_PORT_3000_TCP_ADDR", "AEROSPIKE_SERVER_PORT_3000_TCP_PORT") ::
      ("AEROSPIKE_SERVER_PORT_3001_TCP_ADDR", "AEROSPIKE_SERVER_PORT_3001_TCP_PORT") ::
      ("AEROSPIKE_SERVER_PORT_3002_TCP_ADDR", "AEROSPIKE_SERVER_PORT_3002_TCP_PORT") ::
      ("AEROSPIKE_SERVER_PORT_3003_TCP_ADDR", "AEROSPIKE_SERVER_PORT_3003_TCP_PORT") ::
      Nil
    ).map(x => x match {
      case (h, p) => (sys.env.getOrElse(h, ""), sys.env.getOrElse(p, ""))
    }) filter (x => x match {
      case (_, "") => false
      case ("", _) => false
      case _ => true
    }) map (x => x match {
      case (h, p) => Host(h, p.toInt)
    })
  }

  it should "put and get values" in {

    val settings = Settings(host = hosts)

    val client = Client(settings)
    val key = new Key("test", "teste", "testee")
    val nickanme = new Bin("nickname", new Value("tkrs"))
    val attribute = new Bin(
      "attribute", new Value(
        Map("attr" -> List(
          "100-1000",
          "japan",
          "tokyo"))))
    val favorite = new Bin("favorite", new Value(
      List(
        Map("programming" -> List("rust", "scala", "haskell"))
      )
    ))
    val allData = new Bin("data", new Value(
      List("string", true, null, 1e9, (1L << 63) - 1, 0.1234568.toFloat, (1 << 33) - 1, Array(0x02.toByte))
    ))

    {
      val f = client.put(key, nickanme, attribute, favorite, allData).run
      f onSuccess {
        case \/-(_) => assert(true)
        case -\/(_) => fail()
      }
      f onFailure {
        case e => fail()
      }
      Await.result(f, Duration(500, "millis"))
    }

    {
      val f = client.get(key).run
      f onSuccess {
        case \/-(_) => assert(true)
        case -\/(_) => fail()
      }
      f onFailure {
        case e => fail()
      }
      Await.result(f, Duration(500, "millis"))
    }

    client.close()

  }

  it should "Error result if unregister key & namespace & set" in {

    val settings = Settings(host = hosts)
    val client = Client(settings)

    {
      val key = new Key("test", "teste", "not")
      val f = client.get(key).run
      f onSuccess {
        case \/-(_) => assert(false)
        case -\/(_) => assert(true)
      }
      f onFailure {
        case e => fail()
      }
      Await.result(f, Duration(500, "millis"))
    }

    {
      val key = new Key("test", "not", "not")
      val f = client.get(key).run
      f onSuccess {
        case \/-(_) => assert(false)
        case -\/(_) => assert(true)
      }
      f onFailure {
        case e => fail()
      }
      Await.result(f, Duration(500, "millis"))
    }

    {
      val key = new Key("not", "not", "not")
      val f = client.get(key).run
      f onSuccess {
        case \/-(_) => assert(false)
        case -\/(_) => assert(true)
      }
      f onFailure {
        case e => fail()
      }
      Await.result(f, Duration(500, "millis"))
    }

    client.close()

  }

  it should "throw java.net.ConnectException if specify a incorrect host" in {
    try {
      val errHosts = Host("127.0.0.1", 9090) :: Nil
      val settings = Settings(host = errHosts)
      val client = Client(settings)
      fail("Unexpected connection")
    } catch {
      case _ => assert(true)
    }
  }

}
